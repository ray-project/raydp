"""Exercise the submit launcher without requiring a JVM or Ray cluster.

Run with: python3 -m unittest discover -s tests -p 'test_raydp_submit.py'
"""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / 'bin' / 'raydp-submit'
MAIN_CLASS = 'org.apache.spark.deploy.SparkSubmit'


class RayDPSubmitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.capture = self.root / 'argv'
        self.write_executable('python3', '''#!/bin/sh
case "$2" in
  *"from raydp import versions"*)
    printf '%s\\n' log4j2 log4j.configurationFile log4j2.xml log4j2 ray.log4j.config.file.name log4j2.xml ;;
  *"raydp_agent_path"*) printf '%s\\n' /test/raydp-agent.jar ;;
  *) echo 'Unexpected Python invocation' >&2; exit 1 ;;
esac
''')
        self.write_executable('java/bin/java', '''#!/bin/sh
printf '%s\\0' "$@" > "$CAPTURE_ARGV"
''')
        self.env = dict(os.environ, PATH=str(self.root) + os.pathsep + os.defpath,
                        JAVA_HOME=str(self.root / 'java'), SPARK_HOME='/test/spark',
                        RAYDP_HOME='/test/raydp', RAY_HOME='/test/ray',
                        RAY_DRIVER_NODE_IP='192.0.2.10', CAPTURE_ARGV=str(self.capture))

    def write_executable(self, name, content):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        path.chmod(0o755)

    def launch(self, args):
        result = subprocess.run(['bash', str(SCRIPT)] + args, env=self.env,
                                cwd=self.root, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        return self.capture.read_bytes().decode().split('\0')[:-1]

    def test_application_arguments_and_ray_conf(self):
        cases = [
            ([], 'main.py', []),
            ([], 'main.py', ['--parameter_file=tmp_param.json']),
            (['--py-files', 'lib.zip'], 'hdfs:///app/main.py',
             ['--name', 'value with spaces', '', '--conf', 'business.key=value', '*.csv']),
            (['--class', 'example.Main'], 'app.jar', ['--input', 'input path']),
        ]
        for use_ray_conf in (False, True):
            for options, resource, business_args in cases:
                with self.subTest(ray_conf=use_ray_conf, resource=resource, args=business_args):
                    ray_conf = ['--ray-conf', '/test/ray config.conf'] if use_ray_conf else []
                    original = options + [resource] + business_args
                    java_args = self.launch(ray_conf + original)
                    main_index = java_args.index(MAIN_CLASS)
                    self.assertNotIn('--ray-conf', java_args)
                    self.assertEqual('-Dray.config-file=/test/ray config.conf' in
                                     java_args[:main_index], use_ray_conf)
                    self.assertEqual(java_args[main_index + 1:], [
                        '--master', 'ray',
                        '--conf', 'spark.ray.log4j.config.file.name=log4j2.xml',
                        '--conf', 'spark.driver.host=192.0.2.10',
                        '--conf', 'spark.driver.bindAddress=192.0.2.10',
                    ] + original)

    def test_explicit_driver_addresses_follow_generated_defaults(self):
        for key, value in [('spark.driver.host', 'driver.example'),
                           ('spark.driver.bindAddress', '0.0.0.0')]:
            with self.subTest(key=key):
                args = self.launch(['--conf', key + '=' + value, 'main.py'])
                self.assertLess(args.index(key + '=192.0.2.10'), args.index(key + '=' + value))
                self.assertLess(args.index(key + '=' + value), args.index('main.py'))


if __name__ == '__main__':
    unittest.main()

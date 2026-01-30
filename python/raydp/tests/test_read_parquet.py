import sys
import pytest
from raydp.spark.dataset import read_spark_parquet
import ray.util.client as ray_client


def test_read_parquet_with_file_extensions(spark_on_ray_small, tmp_path):
    if ray_client.ray.is_connected():
        pytest.skip("Skip this test if using ray client")

    spark = spark_on_ray_small
    output_dir = (tmp_path / "spark_parquet_out").as_posix()

    data = [
        ("James", "", "Smith", "36636", "M", 3000),
        ("Michael", "Rose", "", "40288", "M", 4000),
        ("Robert", "", "Williams", "42114", "M", 4000),
        ("Maria", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1),
    ]
    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    spark_df = spark.createDataFrame(data, schema=columns)

    # Spark produces parquet data files plus metadata/CRC files (e.g. _SUCCESS, .crc).
    spark_df.write.mode("overwrite").parquet(output_dir)
    import os

    print("Files under output dir:")
    for root, dirs, files in os.walk(output_dir):
        for name in files:
            print(os.path.join(root, name))

    ds = read_spark_parquet(path=str(f"{output_dir}"))
    assert ds.count() == len(data)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
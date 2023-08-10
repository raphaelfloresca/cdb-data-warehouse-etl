"""
Unit test for the cloud function
"""

from main import pull_to_staging, pull_to_csv


class TestMain:

    def test_data_pull(self):

        assert pull_to_staging() == "Data has been loaded to BigQuery"


    def test_pull_to_csv(self):

        assert pull_to_csv() == "Data has been saved"

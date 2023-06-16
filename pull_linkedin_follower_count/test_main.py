"""
Unit test for the cloud function
"""

from main import pull_to_staging


class TestMain:

    def test_data_pull(self):

        assert pull_to_staging() == "Data has been loaded to BigQuery"

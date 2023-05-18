import requests
import pandas as pd
from helpers.helpers import *
from google.cloud import storage
from PIL import Image
from io import BytesIO

# Main entry point for the cloud function
def pull_from_api(self):
    access_token = return_active_token('ig')
    # Pull list of posts
    url = "https://graph.facebook.com/v16.0/17841456374080288/stories?access_token={}".format(access_token)
    story_list = get_from_api(url, endpoint='data')
    story_list = [d['id'] for d in story_list]

    story_data = []

    for story in story_list:
        story_url = "https://graph.facebook.com/v16.0/{}?fields=timestamp,id,caption,media_url&access_token={}".format(story, access_token)
        story_metadata = get_from_api(story_url)

        # # Download media and upload to Cloud Storage
        # response = requests.get(story_metadata['media_url'])

        # # First use BytesIO to create a file-like object from the request content.
        # image = Image.open(BytesIO(response.content))

        # # Here BytesIO is used to create a new file-like object to save the content of
        # # the image with type converted by Pillow.
        # with BytesIO() as f_png:
        #     image.save(f_png, format="PNG")
        #     # The binary conent can be uploaded to GCP Storage directly.
        #     content = f_png.getvalue()

        # storage_client = storage.Client()
        # bucket = storage_client.bucket("ig-stories")

        # blob_with_path = bucket.blob("images/{}.png".format(story_metadata['id']))
        # # upload_from_string use binary string as the input.
        # # We need to specify the content type otherwise it will be text by default.
        # blob_with_path.upload_from_string(content, content_type="image/png")

        story_data.append(story_metadata)

    df = pd.DataFrame.from_dict(story_data)

    # Write dataframe to csv
    df.to_csv('ig_story_metadata.csv', encoding='utf-8')

    return "Data has been saved"

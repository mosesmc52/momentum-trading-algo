'''
Note: https://www.learnaws.org/2020/12/18/aws-ses-boto3-guide/
'''
import boto3

class AmazonSES(object):

        def __init__(self, region, access_key, secret_key, from, charset = "UTF-8"):
            self.region = region
            self.access_key = access_key
            self.secret_key = secret_key
            self.client = boto3.client("ses",
                                        region_name=self.region,
                                        aws_access_key_id=self.access_key,
                                        aws_secret_access_key=self.secret_key)
                                    )
            self.CHARSET = charset
            self.from = from

        def send_text_email(to, subject, content):


            response = self.client.send_email(
                Destination={
                    "ToAddresses": [
                        to
                    ],
                },
                Message={
                    "Body": {
                        "Text": {
                            "Charset": self.CHARSET,
                            "Data": content,
                        }
                    },
                    "Subject": {
                        "Charset": self.CHARSET,
                        "Data": subject,
                    },
                },
                Source=self.from,
            )

        def send_html_email(to, content):
            response = self.client.send_email(
                Destination={
                    "ToAddresses": [
                        to,
                    ],
                },
                Message={
                    "Body": {
                        "Html": {
                            "Charset": self.CHARSET,
                            "Data": content,
                        }
                    },
                    "Subject": {
                        "Charset": self.CHARSET,
                        "Data": subject,
                    },
                },
                Source=self.from,
            )

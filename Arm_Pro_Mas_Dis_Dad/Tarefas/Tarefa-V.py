!pip install pdfminer
!pip install boto boto3

from collections import Counter
import string
import io
from io import StringIO

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.high_level import extract_text

import os
import base64
import random
from boto.s3.connection import S3Connection
from datetime import datetime
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
conn = S3Connection('key_id', 'access_key')
bucket = conn.get_bucket('datalake-turma5.1')

for key in bucket.list():
    print(key.name.encode('utf-8'))

import boto3

class AmazonS3:
    bucket = None
    bucket_name = None
    aws_secret_access_key = None
    aws_access_key_id = None
    region_name = None
    resource = None
    file_name = None

    def __init__(self):
        ssl._create_default_https_context = ssl._create_unverified_context
        self.bucket = ""
        self.bucket_name = 'datalake-turma5.1'
        self.aws_access_key_id = 'key_id'
        self.aws_secret_access_key = 'access_key'
        self.region_name = 'sa-east-1'
        self.resource = 's3'
        self.file_name = None

    def _connect_s3(self):
            try:
                session = boto3.Session(
                    aws_access_key_id = self.aws_access_key_id,
                    aws_secret_access_key = self.aws_secret_access_key,
                    region_name = self.region_name
                )
                s3 = session.resource(self.resource)
                self.bucket = s3.Bucket(self.bucket_name)
            except Exception as e:
                print(e)

    def _convert_b64_to_image(self, image=None):
        with open(image, "rb") as imageFile:
            file = base64.b64encode(imageFile.read())
        self.file_name  = "{}.xlsx".format("Metadados_JoaoPauloCarneiro")
        image_64_decode = base64.decodebytes(file)
        image_result = open(self.file_name, 'wb')
        image_result.write(image_64_decode)

    def _put_object_bucket(self, request_ticket_receipt_id=None, amazon_path=None):
        path = os.getcwd()
        full_path = os.path.join(path, self.file_name)
        if not amazon_path:
            amazon_path  = "metadados/"
            self.file_name = full_path[len(path)+1:]
            amazon_destiny = amazon_path + self.file_name
        try:
            with open(full_path, 'rb') as data:
                self.bucket.put_object(Key=amazon_destiny, Body=data)
        except Exception as e:
            print(e)
        os.remove(self.file_name)
        url = 'https://{}.s3.amazonaws.com/{}'.format(
            self.bucket_name,
            amazon_destiny
        )
        print(url)
        return url

    def post_receipt_image(self, image=None):
        self._connect_s3()
        self._convert_b64_to_image(image)
        amazon_url = self._put_object_bucket()
        return amazon_url

def convert_pdf_to_txt(path):
    rsrcmgr = PDFResourceManager()
    retstr = io.StringIO()
    codec = 'iso-8859-1'
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, laparams=laparams)
    fp = open(path, 'rb')
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    password = ""
    maxpages = 0
    caching = True
    pagenos = set()

    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages,
                                  password=password,
                                  caching=caching,
                                  check_extractable=True):
        interpreter.process_page(page)

    fp.close()
    device.close()
    text = retstr.getvalue()
    retstr.close()
    return text

from collections import Counter
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk

nltk.download('punkt')
nltk.download('stopwords')

stop_words_pt = set(stopwords.words('portuguese'))

words_to_exclude = ['1','0','nao','sao','x','2','n','p','3','5','y','i','4','b','6','pode','cada','10','and','t','of','the','an','duas','12']

def process_text(text):
    text_clean = re.sub(r'[^\w\s]', '', text).lower()
    words = word_tokenize(text_clean)

    words_filtered = [word for word in words if word not in stop_words_pt]
    word_counts = Counter(words_filtered)

    top_words = word_counts.most_common(50)
    top_words_filtered = [(word, count) for word, count in top_words if word not in words_to_exclude]

    return top_words_filtered[:30]

import pandas as pd

def grava_xlsx(mapa):
    df = pd.DataFrame(mapa, columns=['Palavra', 'Quantidade'])
    df.to_excel('Metadados_JoaoPauloCarneiro.xlsx', index=False, engine='openpyxl')

palavras = convert_pdf_to_txt("ciencia_dados.pdf")
resultado = process_text(palavras)
grava_xlsx(resultado)

ssl._create_default_https_context = ssl._create_unverified_context
AS3 = AmazonS3()
arquivo = 'Metadados_JoaoPauloCarneiro.xlsx'
AS3.post_receipt_image(arquivo)
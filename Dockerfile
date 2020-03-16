
FROM python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir c:\home\kyc_donught
COPY kyc_donught.py /home/kyc_donught/kyc_donught.py
CMD python /home/kyc_donught/kyc_donught.py
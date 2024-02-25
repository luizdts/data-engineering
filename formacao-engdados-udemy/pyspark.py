from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import *
from main.base import PySparkJobInterface
import pyspark.sql.functions as F

class PySparkJob(PySparkJobInterface):

    def init_spark_session(self) -> SparkSession:
        spark = SparkSession.builder.appName("Data Cleaning").getOrCreate()
        return spark


    def filter_medical(self, eligibility: DataFrame, medicals: DataFrame) -> DataFrame:
        return medicals.join(eligibility, eligibility.memberId == medicals.memberId, "left-semi")
    

    def generate_full_name(self, eligibility: DataFrame, medical: DataFrame) -> DataFrame:
            medical_fullname = medical.join(eligibility, ['memberId'], "left")\
                .withColumn('fullName', concat(eligibility.firstName,lit(' '),eligibility.lastName))\
                .select('memberId', 'fullName', 'paidAmount')
            return medical_fullname

    def find_max_paid_member(self, medicals: DataFrame) -> str:
        medical = medicals.sort("paidAmount", ascending=False).first().memberId
        return str(medical)
    
    def find_total_paid_amount(self, medicals: DataFrame) -> int:
        total_amt = medicals.agg({'paidAmount':'sum'})\
                .withColumnRenamed('sum(paidAmount)','total_amt')\
                .first().total_amt
        return total_amt
    
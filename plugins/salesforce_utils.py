from simple_salesforce import Salesforce
from airflow.hooks.base_hook import BaseHook
import json
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
import csv
from tempfile import NamedTemporaryFile
from airflow.hooks.S3_hook import S3Hook


class SalesforceHook(BaseHook, LoggingMixin):
    """
    Forked from Airflow Contrib
    """

    def __init__(self, conn_id, *args, **kwargs):
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson

    def sign_in(self):
        if hasattr(self, "_sf"):
            return self._sf

        sf = Salesforce(
            username=self.connection.login,
            password=self.connection.password,
            security_token=self.extras["security_token"],
            instance_url=self.connection.host,
            sandbox=self.extras.get("sandbox", False),
        )
        self._sf = sf
        return sf

    def make_query(self, query):
        self.sign_in()

        self.log.info("Querying for all objects")

        query = self._sf.query_all(query)

        self.log.info(
            "Received results: Total size: %s; Done: %s",
            query["totalSize"],
            query["done"],
        )

        query = json.loads(json.dumps(query))
        return query


class SalesforceToS3Operator(BaseOperator):
    template_fields = ("sql", "dest_key")
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
        self,
        sql,
        dest_key,
        dest_bucket,
        salesforce_conn_id,
        aws_conn_id="aws_default",
        include_deleted=False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.salesforce_conn_id = salesforce_conn_id
        self.aws_conn_id = aws_conn_id
        self.sql = sql
        self.dest_key = dest_key
        self.dest_bucket = dest_bucket
        self.include_deleted = include_deleted

    def execute(self, context):
        self.log.info("Start executing SalesforceToS3Operator")
        salesforce_hook = SalesforceHook(conn_id=self.salesforce_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        salesforce_conn = salesforce_hook.sign_in()

        self.log.info(f"Going to execute query {self.sql}")

        result = salesforce_conn.query(
            query=self.sql, include_deleted=self.include_deleted
        )
        with NamedTemporaryFile("w", encoding="utf-8") as target_file:
            times_executed = 0
            while True:
                list_result = []
                records_result = result["records"]
                for row in records_result:
                    json_row = json.loads(json.dumps(row))
                    del json_row["attributes"]
                    json_row = {k.lower(): v for k, v in json_row.items()}
                    list_result.append(json_row)
                fieldnames = list(list_result[0])
                csv_writer = csv.DictWriter(target_file, fieldnames=fieldnames)
                if times_executed == 0:
                    csv_writer.writeheader()
                    times_executed = times_executed + 1
                for row in list_result:
                    csv_writer.writerow(row)
                target_file.flush()

                if not result["done"]:
                    result = salesforce_conn.query_more(
                        next_records_identifier=result["nextRecordsUrl"],
                        identifier_is_url=True,
                        include_deleted=self.include_deleted,
                    )
                else:
                    break

            s3_hook.load_file(
                filename=target_file.name,
                key=self.dest_key,
                bucket_name=self.dest_bucket,
                replace=True,
            )


class S3ToSalesforceOperator(BaseOperator):
    template_fields = ("source_key",)

    @apply_defaults
    def __init__(
        self,
        source_key,
        source_bucket,
        salesforce_object,
        salesforce_conn_id,
        api_action,
        aws_conn_id="aws_default",
        batch_size=9000,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.source_key = source_key
        self.source_bucket = source_bucket
        self.salesforce_object = salesforce_object
        self.aws_conn_id = aws_conn_id
        self.salesforce_conn_id = salesforce_conn_id
        self.api_action = api_action
        self.batch_size = batch_size

    def execute(self, context):
        if self.api_action not in ["update", "insert", "delete"]:
            raise Exception(
                "api_action is not update, insert or delete. Check class definition in Dag and try again"
            )

        self.log.info("Getting Connections")
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        salesforce_hook = SalesforceHook(conn_id=self.salesforce_conn_id)
        salesforce_conn = salesforce_hook.sign_in()

        self.log.info("Downloading S3 File")
        with NamedTemporaryFile() as source_csv:
            source_obj = s3_hook.get_key(self.source_key, self.source_bucket)
            with open(source_csv.name, "wb") as opened_source_csv:
                source_obj.download_fileobj(opened_source_csv)

            self.log.info("Replacing special chars")
            with open(source_csv.name, "r+") as opened_source_csv, NamedTemporaryFile(
                "r+"
            ) as sanitized_csv:
                for row in opened_source_csv:
                    sanitized_csv.write(row.replace("\\", ""))

                sanitized_csv.flush()

                self.log.info("Converting CSV to Dict")
                opened_file = open(sanitized_csv.name)
                dict_reader = csv.DictReader(opened_file)
                salesforce_obj = getattr(salesforce_conn.bulk, self.salesforce_object)
                upload_list = []
                for row in dict_reader:
                    upload_list.append(dict(row))
                opened_file.close()

                parsed_upload_list = []
                for i in range(0, len(upload_list), self.batch_size):
                    parsed_upload_list.append(upload_list[i : i + self.batch_size])

                if upload_list == []:
                    self.log.info("Dict doesn't have any records. Skipping.")

                elif self.api_action == "update":
                    self.log.info("Updating salesforce records")
                    for row in parsed_upload_list:
                        response = salesforce_obj.update(row)
                        for row in response:
                            if row["success"] is False:
                                raise Exception(
                                    "Salesforce returned error: " + str(row)
                                )
                    self.log.info("Sucess!")

                elif self.api_action == "insert":
                    self.log.info("Inserting salesforce records")
                    for row in parsed_upload_list:
                        response = salesforce_obj.insert(row)
                        for row in response:
                            if row["success"] is False:
                                raise Exception(
                                    "Salesforce returned error: " + str(row)
                                )
                    self.log.info("Sucess!")

                elif self.api_action == "delete":
                    self.log.info("Deleting salesforce records")
                    for row in parsed_upload_list:
                        response = salesforce_obj.delete(row)
                        for row in response:
                            if row["success"] is False:
                                raise Exception(
                                    "Salesforce returned error: " + str(row)
                                )
                    self.log.info("Sucess!")


class SalesforceUtils(AirflowPlugin):
    name = "salesforce_utils"
    operators = [SalesforceToS3Operator, S3ToSalesforceOperator]
    hooks = [SalesforceHook]
    executors = []
    macros = []
    admin_views = []

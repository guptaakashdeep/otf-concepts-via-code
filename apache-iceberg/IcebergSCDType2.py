from pyspark.sql.functions import col, lit
from datetime import datetime


class IcebergScdType2():
    """SCD Type2 implementation for Iceberg Table"""

    def __init__(self, spark, target_table, staged_df, reporting_date,
                    merge_cols, exclude_cols=None, comparison_cols=None,
                    additional_read_filters=None,
                    additional_set_matched_cols=None):
        """
        Initialize the IcebergSCDType2 class.

        Parameters:
        - spark (SparkSession): The Spark session object.
        - target_table (str): The name of the target table.
        - staged_df (DataFrame): The staged DataFrame to be merged.
        - reporting_date (datetime): The reporting date for the merge operation.
        - merge_cols (list): List of columns to be used for merging.
        - exclude_cols (list, optional): List of columns to be excluded from the merge. Defaults to None.
        - comparison_cols (list, optional): List of columns to be used for comparison. Defaults to None.
        - additional_read_filters (str, optional): Additional filters to be applied during the read operation. Defaults to None.
        - additional_set_matched_cols (dict, optional): Additional columns to be set when a match is found. Defaults to None.
        """
        self.exclude_cols = exclude_cols if exclude_cols is not None else []
        self.comparison_cols = comparison_cols if comparison_cols is not None else []
        self.extra_set_vals = additional_set_matched_cols if additional_set_matched_cols is not None else {}
        self.spark = spark
        self.tgt_tbl = target_table
        self.staged_df = staged_df
        self.join_cols = merge_cols
        self.extra_read_filter = additional_read_filters
        self.reporting_date = reporting_date
        self.reporting_date_str = datetime.strftime(reporting_date, "%Y%m%d")

    def _generate_comparison_column_str(self, tgt_tbl_cols):
        """
        Generates a comparison string for columns between source and target tables.
        This method constructs a comparison string that can be used in SQL queries to
        compare columns between a source table (src) and a target table (tgt). The comparison
        string is generated based on the columns specified in `self.comparison_cols`. If
        `self.comparison_cols` is not provided, it will use all columns from the target table
        excluding those specified in `self.exclude_cols`.
        Args:
            tgt_tbl_cols (list): List of column names in the target table.
        Returns:
            str: A comparison string where each column comparison is joined by " OR ".
        """
        if self.comparison_cols:
            comparison_str = " OR ".join([f"src.{cname} <> tgt.{cname}" for cname in self.comparison_cols])
        else:
            final_cols = list(set(tgt_tbl_cols) - set(self.exclude_cols))
            comparison_str = " OR ".join([f"src.{cname} <> tgt.{cname}" for cname in final_cols])

        return comparison_str

    def _generate_join_str(self):
        """
        Generates a join condition string for SQL queries.

        This method constructs a join condition string by iterating over the 
        columns specified in `self.join_cols`. For each column, it creates a 
        condition in the format `tgt.<column> = src.<column>_mk` and joins 
        these conditions with ' and '.

        Returns:
            str: A string representing the join condition for SQL queries.
        """
        join_condn = [f"tgt.{jcol} = src.{jcol}_mk" for jcol in self.join_cols]
        return ' and '.join(join_condn)

    def _generate_set_str(self):
        """
        Generates a SQL SET clause string for updating records.

        This method constructs a default SET clause string that sets the 
        'valid_flag' to 'N' and 'valid_to_date' to the current timestamp. 
        If additional set values are provided in the 'extra_set_vals' dictionary, 
        they are appended to the default SET clause.

        Returns:
            str: A SQL SET clause string.
        """
        # Default set values
        set_str = "tgt.valid_flag='N', tgt.valid_to_date=current_timestamp()"
        if self.extra_set_vals:
            additional_set_str = ",".join([f"tgt.{cname}={value}" for cname,value in self.extra_set_vals.items()])
            set_str = f"{set_str},{additional_set_str}"
        return set_str

    def execute_scd2(self):
        """
        Executes the Slowly Changing Dimension Type 2 (SCD2) process.
        This method performs the following steps:
        1. Reads the target table with default and additional filters.
        2. Generates a comparison filter string based on the column order.
        3. Identifies updated records and sets the merge key for these records to None.
        4. Sets the merge key for all staged records to the join key and unions them with updated records.
        5. Creates or replaces a temporary view for the staged table.
        6. Generates join and set strings for the merge operation.
        7. Constructs and executes a SQL MERGE statement to update the target table.
        Attributes:
            spark (SparkSession): The Spark session.
            reporting_date (str): The reporting date for filtering records.
            extra_read_filter (Column): Additional filter to apply when reading the target table.
            tgt_tbl (str): The name of the target table.
            staged_df (DataFrame): The staged DataFrame containing new or updated records.
            join_cols (list): List of columns to join on.
            reporting_date_str (str): The reporting date as a string.
        Returns:
            None
        """
        spark = self.spark
        #TODO: Set Spark configurations here for enabling SPJ

        # Default filters
        read_filters = ((col("day_rk") == self.reporting_date) &
                        (col("valid_flag") == 'Y'))
        if not self.extra_read_filter is None:
            read_filters = read_filters & self.extra_read_filter
        tgt_df = spark.table(self.tgt_tbl).filter(read_filters)
        col_order = tgt_df.columns
        src_df = self.staged_df

        comparison_filter_str = self._generate_comparison_column_str(col_order)

        # step1: Get all the udpated records and set the mergekey for updated records to Null
        staged_updated_records = src_df.alias("src").join(tgt_df.alias("tgt"), on=self.join_cols, how='inner').where(comparison_filter_str)\
                                        .select(*[lit(None).alias(f"{jcol}_mk") for jcol in self.join_cols], "src.*")

        # step2: Set mergeKey of all the staged records to join key and union it with updated records
        merge_key_dict = {f'{jcol}_mk': col(jcol) for jcol in self.join_cols}
        staged_records = src_df.withColumns(merge_key_dict)
        all_staged_df = staged_records.unionByName(staged_updated_records)

        all_staged_df.createOrReplaceTempView("staged_tbl")

        join_str = self._generate_join_str()
        matched_set_str = self._generate_set_str()

        print(matched_set_str)

        # TODO: Need to add additional read filters here also.
        # add partitioning join condition here to enable SPJ
        scd2_merge_statement = f"""MERGE into {self.tgt_tbl} as tgt
                        USING (select * from staged_tbl) as src
                        ON {join_str} and tgt.day_rk=CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST({self.reporting_date_str} AS STRING),'yyyyMMdd'))) AS DATE) and tgt.valid_flag='Y'
                        WHEN MATCHED AND {comparison_filter_str} THEN UPDATE 
                            SET {matched_set_str}
                        WHEN NOT MATCHED THEN INSERT *"""
        
        print(scd2_merge_statement)
        # Merge statement
        spark.sql(scd2_merge_statement)


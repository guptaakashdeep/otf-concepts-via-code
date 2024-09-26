from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime


class IcebergScdType2():
    """SCD Type2 implementation for Iceberg Table"""

    def __init__(self, spark, target_table, staged_df, reporting_date,
                    merge_cols, exclude_cols=[],  comparison_cols=[],
                    additional_read_filters=None,
                    additional_set_matched_cols = {}):
        self.spark = spark
        self.tgt_tbl = target_table
        self.staged_df = staged_df
        self.join_cols = merge_cols
        self.extra_read_filter = additional_read_filters
        self.exclude_cols = exclude_cols
        self.comparison_cols = comparison_cols
        self.reporting_date = reporting_date
        self.reporting_date_str = datetime.strftime(reporting_date, "%Y%m%d")
        self.extra_set_vals = additional_set_matched_cols

    def _generate_comparison_column_str(self, tgt_tbl_cols):
        if self.comparison_cols:
            comparison_str = " OR ".join([f"src.{cname} <> tgt.{cname}" for cname in self.comparison_cols])
        else:
            final_cols = list(set(tgt_tbl_cols) - set(self.exclude_cols))
            comparison_str = " OR ".join([f"src.{cname} <> tgt.{cname}" for cname in final_cols])

        return comparison_str

    def _generate_join_str(self):
        join_condn = [f"tgt.{jcol} = src.{jcol}_mk" for jcol in self.join_cols]
        return ' and '.join(join_condn)

    def _generate_set_str(self):
        # Default set values
        set_str = "tgt.valid_flag='N', tgt.valid_to_date=current_timestamp()"
        if self.extra_set_vals:
            additional_set_str = ",".join([f"tgt.{cname}={value}" for cname,value in self.extra_set_vals.items()])
            set_str = f"{set_str},{additional_set_str}"
        return set_str

    def execute_scd2(self):
        spark = self.spark
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

        # Setting Table Properties to write delete files
        # spark.sql(f"""ALTER TABLE {self.tgt_tbl}
        #             SET TBLPROPERTIES (
        #             'write.delete.mode'='merge-on-read',
        #             'write.update.mode'='merge-on-read',
        #             'write.merge.mode'='merge-on-read',
        #             'format-version'='2',
        #             'write.spark.fanout.enabled'='true'
        #             )""")
        # TODO: Need to add additional read filters here also.
        scd2_merge_statement = f"""MERGE into {self.tgt_tbl} as tgt
                        USING (select * from staged_tbl) as src
                        ON {join_str} and tgt.day_rk=CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST({self.reporting_date_str} AS STRING),'yyyyMMdd'))) AS DATE) and tgt.valid_flag='Y'
                        WHEN MATCHED AND {comparison_filter_str} THEN UPDATE 
                            SET {matched_set_str}
                        WHEN NOT MATCHED THEN INSERT *"""
        
        print(scd2_merge_statement)
        # Merge statement
        spark.sql(scd2_merge_statement)


from typing import List, Dict
import json
from pyspark.sql import SparkSession, DataFrame


class IcebergScdType2:
    """SCD Type2 implementation for Iceberg Table"""

    def __init__(
        self,
        spark: SparkSession,
        target_table: str,
        staged_df: DataFrame,
        merge_cols: List[str],
        /,
        matched_set_vals: Dict[str, str],
        tgt_read_filter: Dict[str, str],
        *,
        col_order: List[str] = [],
        exclude_cols: List[str] = [],
        comparison_cols: List[str] = [],
        handle_src_absent=False,
        baselined=True,
        scd2_cols: List[str] = [],
    ):
        """
        Initialize IcebergScdType2 instance.
        
        Args:
            spark: SparkSession instance
            target_table: Name of the target Iceberg table
            staged_df: DataFrame containing staged data to merge
            merge_cols: List of columns to use for joining/merging
            matched_set_vals: Dictionary of column names and values to set for matched records
            tgt_read_filter: Dictionary containing read filter and alias for target table
            col_order: List specifying column order in target table(optional)
            exclude_cols: List of columns to exclude from comparison (optional)
            comparison_cols: List of columns to use for comparison (optional)
            handle_src_absent: Whether to handle records absent in source (optional)
            baselined: Whether staged data is baselined (optional)
            scd2_cols: List of SCD2 columns required when baselined=False (optional)
        """
        self.spark = spark
        self.tgt_tbl = target_table
        self.staged_df = staged_df
        self.join_cols = merge_cols
        self.read_filter = tgt_read_filter["read_filter"]
        self.user_alias = tgt_read_filter["alias"]
        self.exclude_cols = exclude_cols
        self.comparison_cols = comparison_cols
        self.matched_set_vals = matched_set_vals
        self.col_order = col_order
        self.handle_src_absent = handle_src_absent
        self.baselined = baselined
        self.scd2_cols = self._is_scd2_cols_required(scd2_cols)

    def _is_scd2_cols_required(self, scd2_cols: List[str]) -> List[str]:
        """Check if scd2_cols is required. required in case staged data is not baselined."""
        if not self.baselined:
            if not scd2_cols:
                raise Exception("scd2_cols is required if baselined is set to False")
            return scd2_cols
        return scd2_cols

    def _generate_comparison_column_str(
        self, tgt_tbl_cols: List[str], src_alias="s", tgt_alias="t"
    ):
        """
        Generate comparison column string for detecting changes between source and target records.

        Args:
            tgt_tbl_cols: List of target table column names
            src_alias: Alias for source table (default: "s")
            tgt_alias: Alias for target table (default: "t")
            
        Returns:
            String containing OR-separated comparison conditions for detecting record changes
        """
        if self.comparison_cols:
            if set(self.comparison_cols).intersection(set(self.exclude_cols)):
                raise Exception(
                    "AMBIGOUS ERROR: Common Column present in comparison_cols and exclude_cols."
                )
            final_cols = self.comparison_cols
        else:
            final_cols = list(
                set(tgt_tbl_cols) - set(self.join_cols) - set(self.exclude_cols)
            )
        json_schema = json.loads(
            self.spark.table(self.tgt_tbl).select(*final_cols).schema.json()
        )["fields"]
        compare_coalesced_cols = []
        for d in json_schema:
            if d["type"].lower() == "string":
                compare_coalesced_cols.append(
                    f"coalesce({src_alias}.{d['name']}, '') <> coalesce({tgt_alias}.{d['name']}, '')"
                )
            elif d["type"].lower() == "date":
                compare_coalesced_cols.append(
                    f"coalesce({src_alias}.{d['name']}, date('9999-01-01')) <> coalesce({tgt_alias}.{d['name']}, date('9999-01-01'))"
                )
            else:
                compare_coalesced_cols.append(
                    f"coalesce({src_alias}.{d['name']}, 0) <> coalesce({tgt_alias}.{d['name']}, 0)"
                )
        return " OR ".join(compare_coalesced_cols)

    def _generate_join_str(self, src_alias="s", tgt_alias="t", with_mkeys=False):
        """
        Generate join condition string for merging based on join columns.
        """
        if not with_mkeys:
            join_condn = [
                f"{src_alias}.{jcol} = {tgt_alias}.{jcol}" for jcol in self.join_cols
            ]
        else:
            join_condn = [
                f"{tgt_alias}.{jcol} = {src_alias}.{jcol}_mk" for jcol in self.join_cols
            ]
        # return something like tgt.deal_id = src.deal_id and tgt.day_rk = src.day_rk
        return " AND ".join(join_condn)

    def _generate_set_str(self):
        set_str = ",".join(
            [f"tgt.{cname}={value}" for cname, value in self.matched_set_vals.items()]
        )
        return set_str

    def _get_read_filters(self, tgt_alias="t"):
        if (
            not self.user_alias
            or not self.read_filter
            or f"{self.user_alias}." not in self.read_filter
        ):
            raise Exception("alias or read_filter or alias in read_filter is missing.")
        return self.read_filter.replace(f"{self.user_alias}.", f"{tgt_alias}.")

    def _generate_mkeys_str(self, null_vals=False) -> str:
        """Generated Merge Keys based on Join columns.
        e.g., NULL as id1_mk, NULL as id2_mk
        e.g., id1 as id1_mk, id2 as id2_mk"""
        if null_vals:
            return ", ".join([f"NULL as {cname}_mk" for cname in self.join_cols])
        return ", ".join([f"{cname} as {cname}_mk" for cname in self.join_cols])

    def _generate_select_str(self) -> str:
        # If staged data is not baselined
        col_order_str = ",".join(self.col_order)
        # All comparison should come from src (staged data)
        if self.comparison_cols:
            final_cols_set = set(self.comparison_cols + self.scd2_cols)
            col_alias_values = [
                f"s.{cname}" if cname in final_cols_set else f"t.{cname}"
                for cname in self.col_order
            ]
        else:
            raise Exception("All columns comparison without baselined not implemented.")
        baselined_col_order_str = ",".join(col_alias_values)
        return col_order_str, baselined_col_order_str

    def run(self):
        STG_REC = "staged_mem_tbl"
        TGT_TBL = self.tgt_tbl

        if not self.col_order:
            self.col_order = self.spark.table(TGT_TBL).columns

        non_null_mkey = self._generate_mkeys_str()
        null_mkey = self._generate_mkeys_str(null_vals=True)

        # Same comparison string with diffrent aliases
        subq_comparison_filter_str = self._generate_comparison_column_str(
            self.col_order
        )
        matched_comparison_filter_str = self._generate_comparison_column_str(
            self.col_order, src_alias="tgt", tgt_alias="src"
        )

        # Same join condition with different aliases
        subq_join_str = self._generate_join_str()
        merge_join_str = self._generate_join_str(
            src_alias="src", tgt_alias="tgt", with_mkeys=True
        )

        # Create UPDATE SET string for matched records.
        matched_set_str = self._generate_set_str()

        # Target read filters
        # Same read filter condition with diffrent aliases
        subq_read_filter_str = f"AND {self._get_read_filters()}"
        merge_read_filter_str = f"AND {self._get_read_filters(tgt_alias='tgt')}"

        print(matched_set_str)

        if not self.baselined:
            matching_select_str, baselined_select_str = self._generate_select_str()
        else:
            matching_select_str = "*"
            baselined_select_str = "s.*"

        # Create a table in memory from staged dataframe.
        self.staged_df.createOrReplaceTempView(STG_REC)

        # TODO: Check if this can also be updated in case updated records are identified during baselining
        scd2_merge_statement = f"""MERGE INTO {TGT_TBL} as tgt
                USING (
                    SELECT {matching_select_str}, {non_null_mkey} FROM {STG_REC}
                    UNION ALL
                    SELECT {baselined_select_str}, {null_mkey} FROM  {STG_REC} s
                    JOIN {TGT_TBL} t
                    ON {subq_join_str}
                    {subq_read_filter_str}
                    WHERE {subq_comparison_filter_str}
                    ) src
                ON {merge_join_str}
                    {merge_read_filter_str}
                WHEN MATCHED AND ({matched_comparison_filter_str}) THEN
                    UPDATE SET {matched_set_str}
                WHEN NOT MATCHED THEN
                    INSERT *
            """

        if self.handle_src_absent:
            scd2_merge_statement = f"""{scd2_merge_statement}
                WHEN NOT MATCHED BY SOURCE AND tgt.valid_flag = 'Y' THEN
                    UPDATE SET {matched_set_str}"""

        print(scd2_merge_statement)
        # Merge statement
        self.spark.sql(scd2_merge_statement)

        # spark.sql(f"""MERGE INTO {TGT_TBL} as tgt
        #         USING (
        #             -- #### ALL STAGED RECORDS: updated/non-updated/new ####
        #             Select *, id as id_mkey from {STG_REC}
        #             UNION ALL
        #             -- ##### Updated records #####
        #             Select s.*, NULL as id_mkey from  {STG_REC} s
        #             JOIN {TGT_TBL} t
        #             ON s.id = t.id
        #             AND t.valid_flag = 'Y'
        #             AND s.year = t.year
        #             WHERE s.business_vertical <> t.business_vertical
        #             ) src
        #         -- ### IDENTIFYING MATCHED and NOT MATCHED records ###
        #         ON tgt.id = src.id_mkey -- join on generated id_mkey
        #             AND tgt.year = 2025 AND tgt.valid_flag = 'Y'
        #             AND tgt.year = src.year
        #        -- ### matching records (updated + non-updated) but update only one with values updated ###
        #         WHEN MATCHED AND s.business_vertical <> t.business_vertical THEN
        #             UPDATE SET tgt.valid_flag = 'N', tgt.valid_to_date = src.valid_from_date,
        #                 tgt.adjust_flag = CASE WHEN tgt.adjust_flag = 'U' THEN NULL ELSE src.adjust_flag END,
        #                 tgt.status = CASE WHEN tgt.status = 'UNADJUSTED' THEN NULL ELSE src.status END
        #         -- ### Updated records with New value + New RECORDS ###
        #         WHEN NOT MATCHED THEN
        #             INSERT *
        #     """)

# Usage

# scd2 = IcebergScdType2(spark, target_table, staged_df, key_cols, 
#                     matched_set_vals={
#                         "valid_flag": "'N'",
#                         "valid_to_date": f"current_timestamp()",
#                         "adjust_flag": _set_adjust_flag(process_flow),
#                         "status" : _set_status(process_flow)
#                     },
#                     tgt_read_filter={
#                         "alias": "fe",
#                         "read_filter": f"""fe.day_rk=CAST(TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST({REPORTING_DATE} AS STRING),'yyyyMMdd'))) AS DATE)
#                         AND fe.valid_flag='Y'
#                         AND fe.code = 'MR'
#                     },
#                     col_order=col_order, exclude_cols=extra_cols, comparison_cols=comparison_cols, baselined=False,
#                     scd2_cols=list(scd2_cols.keys()))
# scd2.run()

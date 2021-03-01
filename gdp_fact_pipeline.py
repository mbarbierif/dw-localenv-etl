import pandas as pd
import numpy
import os
import csv

from bamboo_lib.helpers import grab_connector
from bamboo_lib.logger import logger, logger
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep


class CountryDimStep(PipelineStep):
	def run_step(self, prev, params):
		logger.info("Creating Country Dimension...")
		
		dim = pd.read_csv("https://datahub.io/core/gdp/r/gdp.csv")
		dim = dim[2306:]

		dim = dim[["Country Name", "Country Code"]].drop_duplicates().reset_index(drop=True)
		dim["country_id"] = dim.index + 1
		dim = dim.rename(columns={"Country Name": "country_name", "Country Code": "country_code"})
		dim = dim[["country_id", "country_name", "country_code"]]

		dim.to_csv("data_temp/dim_country.csv", index=False, quoting=csv.QUOTE_NONNUMERIC)

		return dim


class TransformStep(PipelineStep):
	def run_step(self, prev, params):
		logger.info("Applying Transformations...")

		df = pd.read_csv(prev)
		df = df[2306:]

		# Country Dim
		dim = pd.read_csv("data_temp/dim_country.csv")
		dim_dict = {k:v for (k,v) in zip(dim["country_code"], dim["country_id"])}

		df["Country Code"] = df["Country Code"].map(dim_dict)

		# Tidying Up
		col_names = {
			"Year": "year",
			"Country Code": "country_id",
			"Value": "gdp_value"
		}
		df = df.rename(columns=col_names)

		df = df[["year", "country_id", "gdp_value"]]

		return df


class GDPPipeline(EasyPipeline):
	@staticmethod
	def parameter_list():
		return [
            Parameter("input-file", dtype=str),
			Parameter("output-db", dtype=str),
			Parameter("ingest", dtype=bool)
		]

	@staticmethod
	def steps(params):
		source_connector = grab_connector(__file__, params.get("input-file"))
		db_connector = grab_connector(__file__, params.get("output-db"))

		country_step = CountryDimStep()

		load_country = LoadStep(
			table_name="dim_country",
			connector=db_connector,
			if_exists="drop",
			pk=["country_id"],
			dtype={
				"country_id": "Int64",
				"country_name": "String",
				"country_code": "String"
			},
			nullable_list=[]
		)

		download_step = DownloadStep(
            connector=source_connector,
            force=False
        )

		transform_step = TransformStep()

		load_step = LoadStep(
			table_name="gdp_fact",
			connector=db_connector,
			if_exists="drop",
			pk=["year"],
			dtype={
				"year": "Int64",
				"country_id": "Int64",
				"gdp_value": "Float64"
			},
			nullable_list=[]
		)

		steps = [country_step, load_country, download_step, transform_step, load_step] if params.get("ingest") else [country_step, download_step, transform_step]

		return steps


if __name__ == "__main__":
	gdp_pp = GDPPipeline()
	gdp_pp.run(
		{
			"input-file": "gdp-source",
            "output-db": "clickhouse-local",
            "ingest": False
		}
	)

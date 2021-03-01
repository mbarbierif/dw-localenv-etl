import pandas as pd
import numpy
import os

from bamboo_lib.helpers import grab_connector, query_to_df, grab_connector, grab_parent_dir
from bamboo_lib.logger import logger, logger
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep


class TransformStep(PipelineStep):
	def run_step(self, prev, params):
		logger.info("TransformStep...")
		result = prev

		return result


class TemplatePipeline(EasyPipeline):
	@staticmethod
	def parameter_list():
		return [
            Parameter("input-file", dtype=str),
			Parameter("output-db", dtype=str),
			Parameter("ingest", dtype=bool)
		]

	@staticmethod
	def steps(params):
		parent_dir = os.path.join(grab_parent_dir(__file__))

		download_step = DownloadStep(
            connector=params.get("input-file"),
            connector_path=parent_dir,
            force=False
        )

		transform_step = TransformStep()

		load_step = LoadStep(
			table_name="",
			connector=params.get("output-db"),
			connector_path=parent_dir,
			if_exists="",
			pk=[]
			dtype= ,
			nullable_list=[]
		)

		steps = [download_step, transform_step, load_step] if params.get("ingest") else [download_step, transform_step]

		return steps

if __name__ == "__main__":
	template_pp = TemplatePipeline()
	template_pp.run(
		{
			"input-url": "gdp-source",
            "output-db": "clickhouse-local",
            "ingest": False
		}
	)

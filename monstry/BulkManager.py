import os
import pandas as pd

from monstry.BulkDataBuilder import BulkDataBuilder
from monstry.BulkDataCleaner import BulkDataCleaner
from dotenv import load_dotenv
from pathlib import Path
load_dotenv()


class BulkManager:
    builders = None
    builders_resume = None
    builders_clean = None
    segregacion_criterios_minimos = None

    def __init__(self, bulk_builder: BulkDataBuilder):
        self.bulk = bulk_builder

        # INIT PROCESS

        self.bulk.get_database()

        self.builders = self.bulk.values_to_release_cleanner()

        self.builders_clean = [
            BulkDataCleaner(builder)
            for
            builder
            in
            self.builders
        ]

    def builders_resumen(self):

        self.builders_resume = [
            builder.get_resumen()
            for builder
            in self.builders_clean
        ]

        return self.builders_resume

    def segregacion_criterios_minimos(self):

        for cln in self.builders_clean:
            cln.get_criterios_minimos_resumen()

        self.segregacion_criterios_minimos_dataframes = [
            builders_clean.segregacion_criterios_minimos
            for
            builders_clean
            in
            self.builders_clean
        ]

    def save_data_to(self, **kwargs):

        OUTPUT_DIR = os.getenv("OUTPUT_DIR")

        if not OUTPUT_DIR is None:
            route = Path(OUTPUT_DIR).resolve()

        else:
            route = Path(kwargs.get("base_dir", "./output")).resolve()

        try:

            for df_clean in self.builders_clean:
                df_clean.to_csv_resumen_table(base_dir=route)
                df_clean.total_score_gauge_save(base_dir=route)

        except Exception as e:
            print(e)

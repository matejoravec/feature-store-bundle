import itertools
from typing import List, Tuple

from pyspark.sql import Column
from pyspark.sql import functions as f

from featurestorebundle.feature.Feature import Feature
from featurestorebundle.feature.FeatureList import FeatureList, MasterFeature


class ChangesCalculator:
    def get_changes(self, master_features: List[MasterFeature], entity: str) -> Tuple[List[Column], FeatureList]:
        change_columns = []
        change_features = []

        for master_feature in master_features:
            combinations = list(itertools.combinations(master_feature.time_windows, 2))
            change_columns.extend([self.__change_column(master_feature.name, low, high) for low, high in combinations])
            change_features.extend([self.__change_feature(master_feature.features[0], lo, hi, entity) for lo, hi in combinations])

        return change_columns, FeatureList(change_features)

    def __change_column(self, feature_name: str, low: str, high: str) -> Column:
        time_window_ratio = int(high[:-1]) / int(low[:-1])
        low_col = f.col(feature_name.format(time_window=low))
        high_col = f.col(feature_name.format(time_window=high))

        column_ratio = f.when((low_col == 0) & (high_col == 0), 0).otherwise(low_col / high_col)

        return (column_ratio * time_window_ratio).alias(f"{feature_name}_change_{low}_{high}")

    def __change_feature(self, feature: Feature, low: str, high: str, entity: str) -> Feature:
        metadata = feature.extra

        metadata = {**metadata, "time_window": f"change_{low}_{high}"}

        name = feature.template.name_template.format(**metadata)
        return Feature.from_template(feature.template, entity, name, "double", metadata)
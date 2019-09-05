# Basic Statistics on the D3M MtL DB (9/5/2019)

The number of pipeline runs with unique ids is:

```python
23354
```

The distribution of run phases among pipeline runs is:

```python
{
    'FIT': 13128,
    'PRODUCE': 10226
}
```

The distribution of problem types among pipeline runs is:

| Problem Type                  | Fruequency |
| ----------------------------- | ---------- |
| CLASSIFICATION                | 15520      |
| REGRESSION                    | 2577       |
| SEMISUPERVISED_CLASSIFICATION | 2001       |
| TIME_SERIES_FORECASTING       | 1260       |
| VERTEX_CLASSIFICATION         | 954        |
| GRAPH_MATCHING                | 393        |
| LINK_PREDICTION               | 320        |
| COMMUNITY_DETECTION           | 212        |
| COLLABORATIVE_FILTERING       | 67         |
| CLUSTERING                    | 38         |
| OBJECT_DETECTION              | 12         |

The distribution of problem subtypes among pipeline runs is:

| Problem Subtype | Frequency |
| --------------- | --------- |
| MULTICLASS      | 14836     |
| BINARY          | 3328      |
| UNIVARIATE      | 2216      |
| NONE            | 2090      |
| MULTIVARIATE    | 361       |
| MULTILABEL      | 311       |
| NONOVERLAPPING  | 212       |

The distribution of metric types among pipeline runs is:

| Metric Type                        | Frequency |
| ---------------------------------- | --------- |
| F1_MACRO                           | 4971      |
| ACCURACY                           | 2031      |
| F1                                 | 1492      |
| ROOT_MEAN_SQUARED_ERROR            | 756       |
| MEAN_SQUARED_ERROR                 | 437       |
| MEAN_ABSOLUTE_ERROR                | 363       |
| NORMALIZED_MUTUAL_INFORMATION      | 56        |
| OBJECT_DETECTION_AVERAGE_PRECISION | 5         |

The distribution of score counts per run is:

```python
{
    0: 13243,
    1: 10111
}
```

The distribution of problem inputs across runs is:

```python
{
    1: 23354
}
```

The distribution of target counts across runs is:

```python
{
    1: 22993,
    2: 361
}
```

The distribution of number of datasets per run is:

```python
{
    1: 23354
}
```

The number of distinct datasets (distinctness is determined by dataset digest) found among runs is:

```python
182
```

The distribution of pipeline authors among runs is:

| Source Name       | Frequency |
| ----------------- | --------- |
| isi               | 3367      |
| stanford          | 2913      |
| sri               | 2496      |
| mit               | 2450      |
| nyu               | 2219      |
| ucb               | 2128      |
| cmu               | 1790      |
| tamu              | 1579      |
| stanford_2        | 1507      |
| brown             | 1081      |
| mit17             | 262       |
| SRI               | 241       |
| mit16             | 225       |
| Distil            | 180       |
| mit15             | 118       |
| uncharted         | 112       |
| mit14             | 100       |
| ISI               | 97        |
| mit12             | 72        |
| RPI               | 57        |
| CMU               | 54        |
| mit13             | 53        |
| mit11             | 42        |
| JHU               | 33        |
| mit18             | 18        |
| Cornell           | 17        |
| byu-dml           | 16        |
| ICSI              | 14        |
| Michigan          | 14        |
| VencoreLabs       | 14        |
| MIT_FeatureLabs   | 12        |
| common-primitives | 11        |
| uncharted13       | 10        |
| BBN               | 9         |
| uncharted16       | 9         |
| uncharted14       | 8         |
| uncharted12       | 7         |
| uncharted15       | 6         |
| uncharted17       | 4         |
| uncharted11       | 3         |
| mit21             | 2         |
| uncharted18       | 2         |
| uncharted21       | 2         |

The distribution of prediction column header counts is:

```python
{
    2: 22618,
    0: 427,
    3: 308,
    29: 1
}
```

The number of runs having a predictions column header of `"d3mIndex"` is:

```python
22917
```

The number of normalized metric values found among pipeline runs is:

```python
10111
```

The number of sub-pipelines found among runs is:

```python
0
```

The 40 most commonly used primitives among pipeline runs are:

| Python Path                                                                          | Frequency |
| ------------------------------------------------------------------------------------ | --------- |
| d3m.primitives.data_transformation.extract_columns_by_semantic_types.DataFrameCommon | 37998     |
| d3m.primitives.data_transformation.dataset_to_dataframe.Common                       | 20507     |
| d3m.primitives.data_transformation.construct_predictions.DataFrameCommon             | 17618     |
| d3m.primitives.data_transformation.denormalize.Common                                | 16898     |
| d3m.primitives.data_transformation.column_parser.DataFrameCommon                     | 16785     |
| d3m.primitives.data_cleaning.imputer.SKlearn                                         | 9281      |
| d3m.primitives.data_transformation.cast_to_type.Common                               | 6612      |
| d3m.primitives.data_transformation.to_numeric.DSBOX                                  | 5160      |
| d3m.primitives.data_transformation.conditioner.Conditioner                           | 3898      |
| d3m.primitives.data_preprocessing.dataset_text_reader.DatasetTextReader              | 3896      |
| d3m.primitives.data_preprocessing.robust_scaler.SKlearn                              | 3396      |
| d3m.primitives.classification.random_forest.SKlearn                                  | 2842      |
| d3m.primitives.data_preprocessing.time_series_to_list.DSBOX                          | 2548      |
| d3m.primitives.feature_extraction.random_projection_timeseries_featurization.DSBOX   | 2548      |
| d3m.primitives.classification.gradient_boosting.SKlearn                              | 2093      |
| d3m.primitives.data_preprocessing.do_nothing.DSBOX                                   | 2015      |
| d3m.primitives.schema_discovery.profiler.DSBOX                                       | 1812      |
| d3m.primitives.data_preprocessing.mean_imputation.DSBOX                              | 1790      |
| d3m.primitives.data_transformation.one_hot_encoder.SKlearn                           | 1780      |
| d3m.primitives.classification.xgboost_gbtree.DataFrameCommon                         | 1723      |
| d3m.primitives.classification.extra_trees.SKlearn                                    | 1673      |
| d3m.primitives.data_preprocessing.encoder.DSBOX                                      | 1411      |
| d3m.primitives.data_cleaning.cleaning_featurizer.DSBOX                               | 1264      |
| d3m.primitives.data_transformation.horizontal_concat.DataFrameConcat                 | 1260      |
| d3m.primitives.classification.general_relational_dataset.GeneralRelationalDataset    | 1210      |
| d3m.primitives.data_transformation.simple_column_parser.DataFrameCommon              | 1078      |
| d3m.primitives.feature_construction.corex_text.DSBOX                                 | 1065      |
| d3m.primitives.time_series_classification.k_neighbors.Kanine                         | 909       |
| d3m.primitives.classification.gaussian_classification.MeanBaseline                   | 808       |
| d3m.primitives.normalization.denormalize.DSBOX                                       | 800       |
| d3m.primitives.semisupervised_classification.iterative_labeling.AutonBox             | 778       |
| d3m.primitives.data_transformation.one_hot_encoder.DistilOneHotEncoder               | 772       |
| d3m.primitives.classification.logistic_regression.SKlearn                            | 771       |
| d3m.primitives.classification.k_neighbors.SKlearn                                    | 759       |
| d3m.primitives.regression.xgboost_gbtree.DataFrameCommon                             | 747       |
| d3m.primitives.data_transformation.encoder.DistilTextEncoder                         | 746       |
| d3m.primitives.classification.bernoulli_naive_bayes.SKlearn                          | 714       |
| d3m.primitives.feature_construction.deep_feature_synthesis.SingleTableFeaturization  | 656       |
| d3m.primitives.feature_selection.generic_univariate_select.SKlearn                   | 632       |
| d3m.primitives.classification.decision_tree.SKlearn                                  | 625       |

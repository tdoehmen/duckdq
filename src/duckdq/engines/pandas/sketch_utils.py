import numpy as np
from datasketches import kll_ints_sketch, kll_floats_sketch, tgt_hll_type, hll_sketch

DEFAULT_SKETCH_SIZE = 2048
DEFAULT_HLL_K = 14
DEFAULT_HLL_TYPE = tgt_hll_type.HLL_8

def calculate_sketch_statistics(data):
    columns = list(data.columns)
    types = list(data.dtypes)

    stats_dict = {}
    for column, type in zip(columns, types):
        if type in [np.int32, np.int64, np.float64]:
            data_col = data[column].to_numpy()
            if data[column].dtype in [np.int32, np.int64]:
                kll = kll_ints_sketch(2048)
            elif data[column].dtype == np.float64:
                kll = kll_floats_sketch(2048)
            kll.update(data_col)
            stat_values = kll.get_quantiles([0.05,0.25,0.5,0.75,0.95])
            stat_names = ["0.05", "Q1", "Median", "Q3", "0.95"]

            hll = hll_sketch(DEFAULT_HLL_K, DEFAULT_HLL_TYPE)
            hll.update(data_col) #works with local fork (np.array extension)
            approx_distinct_count = hll.get_estimate()
            stat_values.append(round(approx_distinct_count))
            stat_names.append("Distinct Count")

            stat_pairs = [list(i) for i in zip(stat_names,stat_values)]
            stats_dict[column] = stat_pairs

    return stats_dict

def calculate_sketch_statistics_np(np_arr):
    columns = np_arr.keys()
    stats_dict = {}
    for column in columns:
        type = np_arr[column].dtype
        if type in [np.int32, np.int64, np.float64]:
            data_col = np_arr[column]
            if type in [np.int32, np.int64]:
                kll = kll_ints_sketch(2048)
            elif type == np.float64:
                kll = kll_floats_sketch(2048)
            kll.update(data_col)
            quantiles = kll.get_quantiles([0.05,0.25,0.5,0.75,0.95])
            quantile_names = ["0.05", "Q1", "Median", "Q3", "0.95"]
            stat_pairs = [list(i) for i in zip(quantile_names,quantiles)]
            stats_dict[column] = stat_pairs

    return stats_dict
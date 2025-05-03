def build_grid_result(results, x_bin_count, y_bin_count, end_time):
    strikes_grid_result = tuple(
        (
            result['rx'],
            y_bin_count - result['ry'],
            result['strike_count'],
            -(end_time - result['timestamp']).seconds
        ) for result in results if 0 <= result['rx'] < x_bin_count and 0 < result['ry'] <= y_bin_count
    )
    return strikes_grid_result

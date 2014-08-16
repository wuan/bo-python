from . import histogram, strike, strike_grid


def strike_query():
    from blitzortung import INJECTOR

    return INJECTOR.get(strike.StrikeQuery)


def strike_grid_query():
    from blitzortung import INJECTOR

    return INJECTOR.get(strike_grid.StrikeGridQuery)


def histogram_query():
    from blitzortung import INJECTOR

    return INJECTOR.get(histogram.HistogramQuery)
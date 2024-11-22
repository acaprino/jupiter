import pandas_ta as ta

from misc_utils.enums import Indicators

STOCHASTIC_K = Indicators.STOCHASTIC_K.name
STOCHASTIC_D = Indicators.STOCHASTIC_D.name
SUPERTREND = Indicators.SUPERTREND.name
MOVING_AVERAGE = Indicators.MOVING_AVERAGE.name


def moving_average(window, df):
    if len(df) < window:
        raise ValueError(f"Not enough values in data frames: required {window} and {len(df)} values in")

    df[MOVING_AVERAGE + '_' + str(window)] = ta.sma(df['HA_close'], length=window)


def stochastic(k_period, d_period, smooth_k, df):
    if not {'HA_high', 'HA_low', 'HA_close'}.issubset(df.columns):
        raise ValueError("DataFrame must contain 'High', 'Low', and 'Close' columns")

        # Calculate the Stochastic Oscillator
    stochastic_values = ta.stoch(high=df['HA_high'], low=df['HA_low'], close=df['HA_close'], k=k_period, d=d_period, smoothK=smooth_k)

    suffix = str(k_period) + '_' + str(d_period) + '_' + str(smooth_k)

    # Add the Stochastic Oscillator to the DataFrame
    df[STOCHASTIC_K + '_' + suffix] = stochastic_values['STOCHk_' + suffix]
    df[STOCHASTIC_D + '_' + suffix] = stochastic_values['STOCHd_' + suffix]


def supertrend(period, multiplier, df):
    if not {'HA_high', 'HA_low', 'HA_close'}.issubset(df.columns):
        raise ValueError("DataFrame must contain 'High', 'Low', and 'Close' columns")

    # Calculate the Supertrend
    _supertrend = ta.supertrend(high=df['HA_high'], low=df['HA_low'], close=df['HA_close'], length=period, multiplier=multiplier)

    # Correctly format the column name to match pandas_ta output
    formatted_col_name = 'SUPERT_' + str(period) + '_' + f"{multiplier:.1f}"

    suffix = str(period) + '_' + str(multiplier)

    # Add the Supertrend to the DataFrame
    df[SUPERTREND + '_' + suffix] = _supertrend[formatted_col_name]


def average_true_range(length, df):
    if not {'HA_high', 'HA_low', 'HA_close'}.issubset(df.columns):
        raise ValueError("DataFrame must contain 'High', 'Low', and 'Close' columns")

    df['ATR' + '_' + str(length)] = ta.atr(high=df['HA_high'], low=df['HA_low'], close=df['HA_close'], length=length)

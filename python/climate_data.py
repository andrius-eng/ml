"""Prepare climate regression data from Lithuania ERA5 country daily weather.

Reads the country_daily_weather.csv produced by the lithuania_weather_analysis DAG,
engineers three temporal features, and writes a chronological train/test split.

Features
--------
sin_doy   = sin(2π · day_of_year / 365)   annual seasonality (sine phase)
cos_doy   = cos(2π · day_of_year / 365)   annual seasonality (cosine phase)
year_norm = (year − 1991) / 30            long-term warming trend

Target: temperature_2m_mean (°C), stored as column ``y``.

The split is chronological — years before ``--test-from-year`` go to the
training set; that year and later form the held-out test set.
"""

from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd


def aggregate_to_country(df: pd.DataFrame) -> pd.DataFrame:
    """Average city-level rows to one country row per day."""
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    return (
        df.groupby('time', as_index=False)[['temperature_2m_mean', 'precipitation_sum']]
        .mean()
    )


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df['year'] = df['time'].dt.year
    doy = df['time'].dt.day_of_year.to_numpy(dtype=np.float32)
    df['sin_doy'] = np.sin(2 * np.pi * doy / 365).astype(np.float32)
    df['cos_doy'] = np.cos(2 * np.pi * doy / 365).astype(np.float32)
    df['year_norm'] = ((df['year'] - 1991) / 30).astype(np.float32)
    return df[['year', 'sin_doy', 'cos_doy', 'year_norm', 'temperature_2m_mean']].rename(
        columns={'temperature_2m_mean': 'y'}
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Engineer climate regression features and produce train/test splits'
    )
    parser.add_argument(
        '--input',
        type=str,
        default='python/output/weather/raw_daily_weather.csv',
        help='raw_daily_weather.csv (city-level) produced by the lithuania_weather_analysis DAG',
    )
    parser.add_argument(
        '--train-output',
        type=str,
        default='python/output/climate/climate_train.csv',
    )
    parser.add_argument(
        '--test-output',
        type=str,
        default='python/output/climate/climate_test.csv',
    )
    parser.add_argument(
        '--test-from-year',
        type=int,
        default=2023,
        help='Hold out this year and later as the test set',
    )
    args = parser.parse_args()

    raw = pd.read_csv(args.input)
    country = aggregate_to_country(raw)
    features = build_features(country)

    train_df = features[features['year'] < args.test_from_year].drop(columns=['year'])
    test_df = features[features['year'] >= args.test_from_year].drop(columns=['year'])

    os.makedirs(os.path.dirname(args.train_output) or '.', exist_ok=True)
    os.makedirs(os.path.dirname(args.test_output) or '.', exist_ok=True)
    train_df.to_csv(args.train_output, index=False)
    test_df.to_csv(args.test_output, index=False)

    print(f'Climate train set : {len(train_df):,} rows  ({1991}–{args.test_from_year - 1})  → {args.train_output}')
    print(f'Climate test set  : {len(test_df):,} rows  ({args.test_from_year}+)  → {args.test_output}')


if __name__ == '__main__':
    main()

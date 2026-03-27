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
import json
import os

import numpy as np
import pandas as pd


def aggregate_to_country(df: pd.DataFrame) -> pd.DataFrame:
    """Average city-level rows to one country row per day."""
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    value_cols = [
        c
        for c in [
            'temperature_2m_mean',
            'precipitation_sum',
            'snowfall_sum',
            'sunshine_duration',
            'wind_speed_10m_max',
            'et0_fao_evapotranspiration',
        ]
        if c in df.columns
    ]
    return (
        df.groupby('time', as_index=False)[value_cols]
        .mean()
    )


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values('time').reset_index(drop=True)
    df['year'] = df['time'].dt.year
    doy = df['time'].dt.day_of_year.to_numpy(dtype=np.float32)
    df['sin_doy'] = np.sin(2 * np.pi * doy / 365).astype(np.float32)
    df['cos_doy'] = np.cos(2 * np.pi * doy / 365).astype(np.float32)
    df['year_norm'] = ((df['year'] - 1991) / 30).astype(np.float32)

    feature_cols = ['sin_doy', 'cos_doy', 'year_norm']
    if 'precipitation_sum' in df.columns:
        df['precip_log1p'] = np.log1p(df['precipitation_sum'].clip(lower=0)).astype(np.float32)
        feature_cols.append('precip_log1p')
    if 'snowfall_sum' in df.columns:
        df['snow_log1p'] = np.log1p(df['snowfall_sum'].clip(lower=0)).astype(np.float32)
        feature_cols.append('snow_log1p')
    if 'sunshine_duration' in df.columns:
        # Convert seconds/day to 0..1 fraction of day for stable scale.
        df['sunshine_frac_day'] = (df['sunshine_duration'] / 86400.0).astype(np.float32)
        feature_cols.append('sunshine_frac_day')
    if 'wind_speed_10m_max' in df.columns:
        df['wind_norm'] = (df['wind_speed_10m_max'] / 30.0).astype(np.float32)
        feature_cols.append('wind_norm')
    if 'et0_fao_evapotranspiration' in df.columns:
        df['et0_norm'] = (df['et0_fao_evapotranspiration'] / 10.0).astype(np.float32)
        feature_cols.append('et0_norm')

    out = df[['year', *feature_cols, 'temperature_2m_mean']].rename(columns={'temperature_2m_mean': 'y'})
    return out.dropna().reset_index(drop=True)


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
        '--feature-columns-output',
        type=str,
        default='python/output/climate/feature_columns.json',
        help='JSON list describing model input column order',
    )
    parser.add_argument(
        '--feature-defaults-output',
        type=str,
        default='python/output/climate/feature_defaults.json',
        help='JSON object with mean defaults per feature for inference fallback',
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
    feature_cols = [c for c in features.columns if c not in ('year', 'y')]
    feature_defaults = {c: float(features[c].mean()) for c in feature_cols}

    train_df = features[features['year'] < args.test_from_year].drop(columns=['year'])
    test_df = features[features['year'] >= args.test_from_year].drop(columns=['year'])

    os.makedirs(os.path.dirname(args.train_output) or '.', exist_ok=True)
    os.makedirs(os.path.dirname(args.test_output) or '.', exist_ok=True)
    os.makedirs(os.path.dirname(args.feature_columns_output) or '.', exist_ok=True)
    os.makedirs(os.path.dirname(args.feature_defaults_output) or '.', exist_ok=True)
    train_df.to_csv(args.train_output, index=False)
    test_df.to_csv(args.test_output, index=False)
    with open(args.feature_columns_output, 'w', encoding='utf-8') as f:
        json.dump(feature_cols, f, indent=2)
    with open(args.feature_defaults_output, 'w', encoding='utf-8') as f:
        json.dump(feature_defaults, f, indent=2, sort_keys=True)

    print(f'Climate train set : {len(train_df):,} rows  ({1991}–{args.test_from_year - 1})  → {args.train_output}')
    print(f'Climate test set  : {len(test_df):,} rows  ({args.test_from_year}+)  → {args.test_output}')
    print(f'Feature columns   : {", ".join(feature_cols)}')


if __name__ == '__main__':
    main()

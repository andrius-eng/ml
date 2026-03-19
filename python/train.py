"""Simple example trainer using PyTorch + MLflow.

This script is intentionally minimal so it can be used as a demo in Airflow
or any orchestration system.

Usage:
  python python/train.py --epochs 3 --lr 0.01

It trains a tiny linear model on synthetic data and logs metrics to MLflow.
"""

import argparse
import os

import mlflow
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim

from model import LinearModel, make_synthetic_data


def train(
    epochs: int,
    lr: float,
    batch_size: int,
    tracking_uri: str,
    data_path: str | None = None,
    model_path: str | None = None,
    metrics_path: str | None = None,
):
    mlflow.set_tracking_uri(tracking_uri)

    if data_path:
        import pandas as pd

        df = pd.read_csv(data_path)
        x = torch.from_numpy(df['x'].to_numpy(dtype=np.float32)).reshape(-1, 1)
        y = torch.from_numpy(df['y'].to_numpy(dtype=np.float32)).reshape(-1, 1)
    else:
        x_np, y_np = make_synthetic_data()
        x = torch.from_numpy(x_np)
        y = torch.from_numpy(y_np)

    model = LinearModel()
    optimizer = optim.SGD(model.parameters(), lr=lr)
    criterion = nn.MSELoss()

    with mlflow.start_run(run_name="train-linear-model"):
        mlflow.log_params({"epochs": epochs, "lr": lr, "batch_size": batch_size})

        metrics = []
        for epoch in range(1, epochs + 1):
            permutation = torch.randperm(x.size(0))
            epoch_loss = 0.0

            for i in range(0, x.size(0), batch_size):
                idx = permutation[i : i + batch_size]
                xb, yb = x[idx], y[idx]

                optimizer.zero_grad()
                preds = model(xb)
                loss = criterion(preds, yb)
                loss.backward()
                optimizer.step()

                epoch_loss += loss.item() * xb.size(0)

            epoch_loss /= x.size(0)
            mlflow.log_metric("mse", epoch_loss, step=epoch)
            metrics.append({"epoch": epoch, "mse": epoch_loss})

        if model_path is None:
            model_path = os.path.join(os.getcwd(), "model.pth")
        os.makedirs(os.path.dirname(model_path) or ".", exist_ok=True)
        torch.save(model.state_dict(), model_path)
        mlflow.log_artifact(model_path)

        if metrics_path:
            import csv

            os.makedirs(os.path.dirname(metrics_path) or ".", exist_ok=True)
            with open(metrics_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["epoch", "mse"])
                writer.writeheader()
                writer.writerows(metrics)

    print(f"Training complete. Model saved to {model_path}")
    if metrics_path:
        print(f"Metrics written to {metrics_path}")


def main():
    parser = argparse.ArgumentParser(description="Train a small model and log metrics to MLflow")
    parser.add_argument("--epochs", type=int, default=5, help="Number of training epochs")
    parser.add_argument("--lr", type=float, default=0.01, help="Learning rate")
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size")
    parser.add_argument(
        "--tracking-uri",
        type=str,
        default="./mlruns",
        help="MLflow tracking URI (local path or remote server)",
    )
    parser.add_argument(
        "--data", type=str, default=None, help="Optional CSV data path (x,y columns)"
    )
    parser.add_argument(
        "--model-path",
        type=str,
        default="python/output/model.pth",
        help="Where to save the trained model",
    )
    parser.add_argument(
        "--metrics-path",
        type=str,
        default="python/output/metrics.csv",
        help="Where to save training metrics",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not run a full training loop; just check imports and setup.",
    )

    args = parser.parse_args()

    if args.dry_run:
        print("Dry run OK: torch and mlflow imported successfully.")
        return

    train(
        args.epochs,
        args.lr,
        args.batch_size,
        args.tracking_uri,
        data_path=args.data,
        model_path=args.model_path,
        metrics_path=args.metrics_path,
    )


if __name__ == "__main__":
    main()

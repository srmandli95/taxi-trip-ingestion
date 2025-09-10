import os, sys, json, time, math, argparse, datetime as dtf
from pathlib import Path
import requests
import pandas as pd


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
RAW = DATA / "raw"

RAW.mkdir(parents=True, exist_ok=True)

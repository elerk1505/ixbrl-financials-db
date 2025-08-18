#!/usr/bin/env python3
import os
from scripts.fetch_company_metadata import main as run

if __name__ == "__main__":
    # just delegates; flags now come from env (see workflow)
    run()

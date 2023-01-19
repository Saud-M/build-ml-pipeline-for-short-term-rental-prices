#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases

Author: Saud
Date: January 2023
"""
import argparse
import logging
import wandb
import pandas as pd
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    """
    Function to download data from W&B, apply basic data cleaning, and logging
    argument:
        args : command line argument to specify artifact information and
            basic cleaning configuration
            --input_artifact: str
            --output_artifact: str
            --output_type: str
            --output_description: str
            --min_price: float
            --max_price: float
    """
    
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("Downloading Artifact")
    artifact_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_path)

    # Drop outliers
    logger.info("Dropping outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    logger.info("Converting last_review to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save the cleaned dataset
    logger.info("Saving the output artifact")
    file_name = "clean_sample.csv"
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    df.to_csv(file_name, index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(file_name)

    logger.info("Logging artifact")
    run.log_artifact(artifact)

    os.remove(file_name)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type= str, ## INSERT TYPE HERE: str, float or int,
        help= "name for the input artifact", ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type= str, ## INSERT TYPE HERE: str, float or int,
        help= "name of the output artifact", ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type= str, ## INSERT TYPE HERE: str, float or int,
        help= "Type of output artifact", ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type= str, ## INSERT TYPE HERE: str, float or int,
        help= "Description for the output artifact", ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type= float, ## INSERT TYPE HERE: str, float or int,
        help= "Minimum price to clean outliers", ## INSERT DESCRIPTION HERE,
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type= float, ## INSERT TYPE HERE: str, float or int,
        help= "Maximum price to clean outliers", ## INSERT DESCRIPTION HERE,
        required=True
    )

    args = parser.parse_args()

    go(args)

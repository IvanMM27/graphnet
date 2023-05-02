# Import(s)
import os

from graphnet.constants import CONFIG_DIR  # Local path to graphnet/configs
from graphnet.data.dataloader import  DataLoader
from graphnet.models import Model
from graphnet.utilities.config import DatasetConfig, ModelConfig

# Configuration
dataset_config_path = f"{CONFIG_DIR}\\datasets\\training_example_data_sqlite.yml"
model_config_path = f"{CONFIG_DIR}\\models\\example_energy_reconstruction_model.yml"

# Build model
model_config = ModelConfig.load(model_config_path)
model = Model.from_config(model_config, trust=True)

# Construct dataloaders
dataset_config = DatasetConfig.load(dataset_config_path)
dataloaders = DataLoader.from_dataset_config(
    dataset_config,
    batch_size=16,
    num_workers=1,
)

from  graphnet.training.callbacks import ProgressBar

from  pytorch_lightning import Trainer

# Configure Trainer
trainer = Trainer(
    gpus=None,
    max_epochs=10,
    callbacks=[ProgressBar()],
    log_every_n_steps=1,
    logger=None,
    strategy="ddp",
)

# Train model
trainer.fit(model, dataloaders)
from graphnet.data.parquet import ParquetDataset

dataset = ParquetDataset(
    path="..\data\examples\parquet\prometheus\prometheus-events.parquet", #Path to the file where the input data is stored
    
    pulsemaps="total",      # Represents the detector response: PMT hits or pulses, in some time window, 
                            # usually triggered by a single neutrino or atmospheric muon interaction.
    
    truth_table="mc_truth", # Name of a table/array that contains the truth-level information associated 
                            # with the pulse series, and should contain the truth labels that you would 
                            # like to reconstruct or classifyruth labels that you would like to reconstruct 
                            # or classify.
    
    features=["sensor_pos_x", "sensor_pos_y", "sensor_pos_z", "t"],         # Names of the columns in your pulse series table(s) 
                                                                            # that you would like to include for training.
    
    truth=["injection_energy", "injection_zenith"],                         # Truth table/array that you would like to include in the dataset.

)

graph = dataset[0]  # torch_geometric.data.Data

print(graph["injection_energy"])

dataset.config.dump("dataset.yml")  # Saves the model and its configuration into a yaml file, it's useful when we want to use it in another run
                                    
# How to load data the dataset:
#   from graphnet.data.dataset import Dataset
#   dataset = Dataset.from_config("..\Path_to_dataset\dataset.yml")
# This will load the same as before



#from graphnet.data import EnsembleDataset
#dataset2 = ParquetDataset(...)
#ensemble_dataset = EsembleDataset([dataset, dataset1])

from graphnet.data.dataloader import DataLoader

dataloader = DataLoader(
    dataset,
    batch_size=128,
    num_workers=1,
)

for batch in dataloader:
    print(batch)

import yaml

def load_config(config_path: str):

    with open(config_path) as yaml_file:
        config = yaml.load(yaml_file, Loader = yaml.Loader)
        return config

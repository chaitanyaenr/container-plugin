## container-plugin

Arcaflow lugin to kill containers in a pod based on [arcaflow-plugin-sdk-python](https://github.com/arcalot/arcaflow-plugin-sdk-python)

### Steps to run the plugin

#### Clone the repository
```
$ git clone https://github.com/chaitanyaenr/container-plugin.git
$ cd container-plugin
```

#### Install the dependencies
```
$ python3.9 -m venv container-plugin
$ source container-plugin/bin/activate
$ pip3.9 install -r requirements.txt
```

#### Run
```
$ python3.9 run_container_plugin.py -f <config_file_location> 
```
**NOTE**: Sample config file can be found [here](https://github.com/chaitanyaenr/container-plugin/blob/main/config_example.yaml)

# smartocean_austevoll_station
This is a standalone module for extracting data from the Austevoll test station data stream.

## Example usage


First build a virtual environment::

    python -m venv venv

You now have a folder ``venv`` containing the empty python virtual environment. Now
populate the ``venv`` folder using your newly created environment with the required libraries for development (this command is for windows, if you use linux then change the paths appropriately):

    .\\venv\\Scripts\\pip install -r .\\requirements.txt


To run the standalone data extractor you can use a commandline like this:

    .\\venv\\Scripts\\python.exe extract_austevoll.py --directory_path=.\\data\\Austevoll_Nord
	
Running this command will read all of the xml files in the data folder and output pandas dataframes in feather format representing the data found in the xml files.

You can then process the dataframe using pandas, or extract the code you need for your own purposes.

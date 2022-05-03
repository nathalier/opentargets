# Open Target Data Pipeline
The script calculates statistics on target-disease associations. 

In order to run the script you would need:
- Python 3
- rsync
- python packages listed in `requirements.txt`
  (run `pip install -r requirements.txt` to install them)

Run the script as `python pipeline.py`.
The result of the calculations can be found in the `data/21.11/output/result.json` file.

*****
Branch `tast-6-draft` contains initial version of the task on defining related targets pairs.
The answer is: 
  Number of target-target pairs sharing at least 2 common deseases: 350414

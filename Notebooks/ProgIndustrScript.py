import os
import sys

module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)

import Prep.utill as utill
import pandas as pd
from Prep import features_utills
#import importlib
import time
import csv
'''
Used to find all the similarities from databases DBLP-ACM and DBLP-Scholar
'''
datasets = ['DBLP-ACM', 'DBLP-Scholar']
# dataset = 'DBLP-ACM'
# dataset = 'DBLP-Scholar'
if __name__ == '__main__':
    for dataset in datasets:
        path_data = os.path.join('..', 'Data', dataset, 'normalized_index')

        dataset_left_name = os.path.join(path_data, 'left.csv')
        dataset_right_name = os.path.join(path_data, 'right.csv')
        dataset_gt_name = os.path.join(path_data, 'gt.csv')

        path_data_dblp_acm = os.path.join(path_data, )

        df1 = pd.read_csv(dataset_left_name)
        df1.set_index('id', inplace=True)

        df2 = pd.read_csv(dataset_right_name)
        df2.set_index('id', inplace=True)

        gt = pd.read_csv(dataset_gt_name)

        gt.columns = ['id_l', 'id_r']
        print(df1.columns)
        gt.head()

        t_blocking = utill.TokenBlockingSchema(df1, df2)
        t_blocking.index()
        pair_iterator = t_blocking.get_pair_iterator(50)

        # %%

        pairs = set()
        for i in pair_iterator:
            pairs.add(i)
        # Saving in csv tests changing number of process  of parallelGetColSim

        #importlib.reload(features_utills)
        process_list = [1,2, 4, 6, 8, 10, 12, 14, 16, 18]
        results = {"Hamming": [], "Levenshtein": [], "Damerau levenshtein": [], "Needleman-Wunsch": [], "Gotoh": [],
                   "Smith-Waterman": [], "Jaccard": [], "Tversky index": [], "Overlap coefficient": [], "Cosine": []
            , "Monge-Elkan": [], "Bag distance": [], "Arithmetic coding": []}

        mapper = {"1": "Hamming", "2": "Levenshtein", "3": "Damerau levenshtein", "4": "Needleman-Wunsch", "5": "Gotoh",
                  "6": "Smith-Waterman"
            , "7": "Jaccard", "8": "Tversky index", "9": "Overlap coefficient", "10": "Cosine", "11": "Monge-Elkan",
                  "12": "Bag distance", "13": "Arithmetic coding"}

        for n in process_list:
            for sim_config in range(1, 14):
                # if __name__ == '__main__':
                t = time.time()
                all_pairs_sim = features_utills.parallelGetColSim(df1, df2, pairs, False, sim_config, False, n)
                t = time.time() - t
                results[mapper.get(str(sim_config))].append(t)
                print(f"{dataset}with {n} process and sim:{sim_config} --> {t}s")


        # Write on csv
        with open(f'{dataset}_speedTest.csv', 'w') as f:
            w = csv.writer(f)
            header = [str(n) for n in process_list]
            header.insert(0, 'Sim_Algorithm')
            w.writerow(header)
            for k, v in results.items():
                aux = v
                aux.insert(0, k)
                w.writerow(aux)

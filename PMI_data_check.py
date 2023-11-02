#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 24 10:08:25 2023

@author: epnzv
"""

import pandas as pd

corn_CF = pd.read_excel('../NA-soy-pricing/data/FY23_Corn_022023.xlsx')
soybean_CF = pd.read_excel('../NA-soy-pricing/data/FY23_Soy_011923.xlsx')

DK_MPI = pd.read_excel('../NA-soy-pricing/data/MPI_data/Dekalb_Corn.xlsx')
AS_MPI = pd.read_excel('../NA-soy-pricing/data/MPI_data/Asgrow_Soybeans.xlsx')
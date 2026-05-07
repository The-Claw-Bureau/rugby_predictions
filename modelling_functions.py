import pandas as pd
import numpy as np


import datetime


from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient


import gzip

from multiprocessing import Pool
from multiprocessing import Pool

import io


import tensorflow as tf
# import tensorflow_addons as tfa
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.regularizers import l1, l2, l1_l2
from tensorflow.keras.utils import to_categorical
from tensorflow.keras.callbacks import LearningRateScheduler, ReduceLROnPlateau
from tensorflow.keras.initializers import HeNormal, GlorotUniform
import tensorflow_probability as tfp


from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.model_selection import train_test_split, KFold, StratifiedKFold
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error, accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, classification_report
from sklearn.utils.class_weight import compute_class_weight

from statsmodels.stats.outliers_influence import variance_inflation_factor


import pickle
import json
from collections import Counter
import itertools
import shap


from azure.storage.blob import ContainerClient
from io import StringIO
import re
import os


import joblib
import sys

from sklearn.compose import ColumnTransformer



all_pcas = [True, False]
all_pca_thresholds = [0.85, 0.9, 0.95, 0.99]

all_learning_rates = [0.001, 0.01, 0.1]

all_batch_sizes = [32, 64, 128]

epochs = [5, 10, 20, 30, 40, 50, 75, 100]

num_hidden_layers = [1, 2, 3, 4, 5]

all_l1_regs = [0.1, 0.01, 0.001, 0.0001]
all_l2_regs = [0.1, 0.01, 0.001, 0.0001]
all_num_nodes =  [256, 128, 64, 32, 16]

all_class_weights_enabled = [False, True]




def add_additional_modelling_features(all_features_df):
    
    all_features_df['win_margin'] = all_features_df[['home_score', 'away_score']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else x[0] - x[1], axis = 1)
    all_features_df['win_margin_sign'] = all_features_df['win_margin'].apply(lambda x: -1 if x < 0 else 1)

    all_features_df['home_win_not_win'] = all_features_df[['home_score', 'away_score']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else 1 if x[0] > x[1] else 0, axis = 1)
    all_features_df['home_away_win'] = all_features_df[['home_score', 'away_score']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else 1 if x[0] > x[1] else 0 if x[0] < x[1] else None, axis = 1)

    all_features_df['delta_error'] = all_features_df[['pre_delta_diff', 'win_margin']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else x[0] - x[1], axis = 1)
    all_features_df['delta_error_abs'] = abs(all_features_df['delta_error'])
    all_features_df['delta_success'] = all_features_df[['pre_delta_diff', 'win_margin']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else 1 if ((x[0] > 0) & (x[1] > 0)) | ((x[0] < 0) & (x[1] < 0)) else 0, axis = 1)


    all_features_df['delta_adjusted_error'] = all_features_df[['pre_delta_diff_adjusted_closebig', 'win_margin']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else x[0] - x[1], axis = 1)
    all_features_df['delta_adjusted_error_abs'] = abs(all_features_df['delta_adjusted_error'])
    all_features_df['delta_adjusted_success'] = all_features_df[['pre_delta_diff_adjusted_closebig', 'win_margin']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else 1 if ((x[0] > 0) & (x[1] > 0)) | ((x[0] < 0) & (x[1] < 0)) else 0, axis = 1)

    all_features_df['p1wm_adjusted_error'] = all_features_df[['p1_winmargin_global_closebig', 'win_margin']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else x[0] - x[1], axis = 1)
    all_features_df['p1wm_adjusted_error_abs'] = abs(all_features_df['p1wm_adjusted_error'])
    all_features_df['p1wm_adjusted_success'] = all_features_df[['p1_winmargin_global_closebig', 'win_margin']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else 1 if ((x[0] > 0) & (x[1] > 0)) | ((x[0] < 0) & (x[1] < 0)) else 0, axis = 1)

    all_features_df['p1_winmargin_compspecific_error'] = all_features_df[['p1_winmargin_compspecific', 'win_margin']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else x[0] - x[1], axis = 1)
    all_features_df['p1_winmargin_compspecific_error_abs'] = abs(all_features_df['p1_winmargin_compspecific_error'])
    all_features_df['p1_winmargin_compspecific_success'] = all_features_df[['p1_winmargin_compspecific', 'win_margin']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else 1 if ((x[0] > 0) & (x[1] > 0)) | ((x[0] < 0) & (x[1] < 0)) else 0, axis = 1)
    
    
    
    # Create category for delta
    all_features_df['expected_delta_category_1'] = all_features_df['pre_delta_diff'].apply(lambda x: None if pd.isna(x) else 'A' if abs(x) <= 3 else 'B')
    all_features_df['expected_delta_category_2'] = all_features_df['pre_delta_diff'].apply(lambda x: None if pd.isna(x) else 'A' if x <= -3 else 'B' if x < -1 else 'C' if x < 1 else 'D' if x < 3 else 'E')
    all_features_df['expected_delta_category_3'] = all_features_df['pre_delta_diff'].apply(lambda x: None if pd.isna(x) else 'A' if x < -7 else 'B' if x < -3 else 'C' if x < 0 else 'D' if x <= 3 else 'E' if x <= 7 else 'F')
    all_features_df['expected_delta_category_4'] = all_features_df['pre_delta_diff'].apply(lambda x: None if pd.isna(x) else 'A' if x < 0 else 'B')

    
    # Create category for delta adjusted
    all_features_df['expected_delta_adjusted_category_1'] = all_features_df['p1_model_global_predeltadiff_adjusted'].apply(lambda x: None if pd.isna(x) else 'A' if abs(x) <= 3 else 'B')
    all_features_df['expected_delta_adjusted_category_2'] = all_features_df['p1_model_global_predeltadiff_adjusted'].apply(lambda x: None if pd.isna(x) else 'A' if x <= -3 else 'B' if x < -1 else 'C' if x < 1 else 'D' if x < 3 else 'E')
    all_features_df['expected_delta_adjusted_category_3'] = all_features_df['p1_model_global_predeltadiff_adjusted'].apply(lambda x: None if pd.isna(x) else 'A' if x < -7 else 'B' if x < -3 else 'C' if x < 0 else 'D' if x <= 3 else 'E' if x <= 7 else 'F')
    all_features_df['expected_delta_adjusted_category_4'] = all_features_df['p1_model_global_predeltadiff_adjusted'].apply(lambda x: None if pd.isna(x) else 'A' if x < 0 else 'B')
    
    
    # Create category for p1 global win margin
    all_features_df['expected_globalwinmargin_category_1'] = all_features_df['p1_winmargin_global_closebig'].apply(lambda x: None if pd.isna(x) else 'A' if abs(x) <= 3 else 'B')
    all_features_df['expected_globalwinmargin_category_2'] = all_features_df['p1_winmargin_global_closebig'].apply(lambda x: None if pd.isna(x) else 'A' if x <= -3 else 'B' if x < -1 else 'C' if x < 1 else 'D' if x < 3 else 'E')
    all_features_df['expected_globalwinmargin_category_3'] = all_features_df['p1_winmargin_global_closebig'].apply(lambda x: None if pd.isna(x) else 'A' if x < -7 else 'B' if x < -3 else 'C' if x < 0 else 'D' if x <= 3 else 'E' if x <= 7 else 'F')
    all_features_df['expected_globalwinmargin_category_4'] = all_features_df['p1_winmargin_global_closebig'].apply(lambda x: None if pd.isna(x) else 'A' if x < 0 else 'B')

    
    all_features_df['expected_p1_winmargin_compspecific_category_1'] = all_features_df['p1_winmargin_compspecific'].apply(lambda x: None if pd.isna(x) else 'A' if x < 0 else 'B')

    
    all_features_df['p1_deltaerror_global_transformed_sqrt_diff'] = all_features_df[['pre_delta_diff_adjusted_closebig', 'p1_deltaerror_global_transformed_sqrt']].apply(lambda x: x.iloc[0] - x.iloc[1], axis = 1)
    all_features_df['p1_deltaerror_global_transformed_root34_diff'] = all_features_df[['pre_delta_diff_adjusted_closebig', 'p1_deltaerror_global_transformed_root34']].apply(lambda x: x.iloc[0] - x.iloc[1], axis = 1)
    all_features_df['p1_winmargin_global_CloseBig_diff'] = all_features_df[['pre_delta_diff_adjusted_closebig', 'p1_winmargin_global_closebig']].apply(lambda x: x.iloc[0] - x.iloc[1], axis = 1)
    all_features_df['p1_winmargin_global_transformed_sqrt_diff'] = all_features_df[['pre_delta_diff_adjusted_closebig', 'p1_winmargin_global_transformed_sqrt']].apply(lambda x: x.iloc[0] - x.iloc[1], axis = 1)
    all_features_df['p1_winmargin_global_transformed_root34_diff'] = all_features_df[['pre_delta_diff_adjusted_closebig', 'p1_winmargin_global_transformed_root34']].apply(lambda x: x.iloc[0] - x.iloc[1], axis = 1)
    all_features_df['p1_winmargin_global_CloseBig_2_diff'] = all_features_df[['pre_delta_diff_adjusted_closebig', 'p1_winmargin_global_closebig_2']].apply(lambda x: x.iloc[0] - x.iloc[1], axis = 1)

    all_features_df['p1_deltaerror_global_closegames_diff_CSWM'] = all_features_df['p1_deltaerror_global_closegames'] - all_features_df['p1_deltaerror_global_closegames']
    all_features_df['p1_winmargin_global_closegames_diff_CSWM'] = all_features_df['p1_winmargin_global_closegames'] - all_features_df['p1_deltaerror_global_closegames']

    # Limit the streaks to 5
    # We could potentially experiment with different values of this
    all_features_df['home_pos_error_streak_adj'] = all_features_df['home_pos_error_streak'].apply(lambda x: 5 if x >= 5 else x)
    all_features_df['away_pos_error_streak_adj'] = all_features_df['away_pos_error_streak'].apply(lambda x: 5 if x >= 5 else x)
    all_features_df['home_neg_error_streak_adj'] = all_features_df['home_neg_error_streak'].apply(lambda x: 5 if x >= 5 else x)
    all_features_df['away_neg_error_streak_adj'] = all_features_df['away_neg_error_streak'].apply(lambda x: 5 if x >= 5 else x)



    # Convert numerical categorical columns to integer so we don't get the 0.0 at the end
    for col in [
    'home_pos_error_streak_adj',
    'home_neg_error_streak_adj',
    'away_pos_error_streak_adj',
    'away_neg_error_streak_adj',
    'features_z_score_cluster',
    'general_features_cluster',
    ]:

        # Convert the column to a nullable integer type
        all_features_df[col] = all_features_df[col].astype('Int64')



    # Need to add these as formulas in the delta calculations
    all_features_df['home_team_pre_delta_mean_last_10'] = all_features_df[['home_pre_delta', 'home_team_pre_delta_mean_last_10']].apply(lambda x: x[0] if pd.isna(x[1]) else x[1], axis = 1 )
    all_features_df['home_team_pre_delta_mean_last_20'] = all_features_df[['home_pre_delta', 'home_team_pre_delta_mean_last_20']].apply(lambda x: x[0] if pd.isna(x[1]) else x[1], axis = 1 )
    all_features_df['away_team_pre_delta_mean_last_10'] = all_features_df[['away_pre_delta', 'away_team_pre_delta_mean_last_10']].apply(lambda x: x[0] if pd.isna(x[1]) else x[1], axis = 1 )
    all_features_df['away_team_pre_delta_mean_last_20'] = all_features_df[['away_pre_delta', 'away_team_pre_delta_mean_last_20']].apply(lambda x: x[0] if pd.isna(x[1]) else x[1], axis = 1 )

    all_features_df['diff_pre_delta_mean_last_10'] = all_features_df[['diff_pre_delta_mean_last_10', 'home_team_pre_delta_mean_last_10', 'away_team_pre_delta_mean_last_10', 'home_team_buffer']].apply(lambda x: (float(x[0]) + float(x[3])) if pd.notna(float(x[0])) else (float(x[1]) + float(x[3])) - float(x[2]), axis = 1 )
    all_features_df['diff_pre_delta_mean_last_20'] = all_features_df[['diff_pre_delta_mean_last_20', 'home_team_pre_delta_mean_last_20', 'away_team_pre_delta_mean_last_20', 'home_team_buffer']].apply(lambda x: (float(x[0]) + float(x[3])) if pd.notna(float(x[0])) else (float(x[1]) + float(x[3])) - float(x[2]), axis = 1 )

    
    def compare_bands(row, home_low_col, home_high_col, away_low_col, away_high_col):
        home_low = row[home_low_col]
        home_high = row[home_high_col]
        away_low = row[away_low_col]
        away_high = row[away_high_col]

        if home_low > away_high:
            return 1
        elif away_low > home_high:
            return -1
        else:
            return 0


    all_features_df['band_2std_comparison_result'] = all_features_df.apply(
        lambda row: compare_bands(
            row,
            'home_lower_band_2std_last_10',
            'home_upper_band_2std_last_10',
            'away_lower_band_2std_last_10',
            'away_upper_band_2std_last_10'
        ), axis=1
    )

    all_features_df['band_1_5std_comparison_result'] = all_features_df.apply(
        lambda row: compare_bands(
            row,
            'home_lower_band_1_5std_last_10',
            'home_upper_band_1_5std_last_10',
            'away_lower_band_1_5std_last_10',
            'away_upper_band_1_5std_last_10'
        ), axis=1
    )


    categorical_features = [
    'level', 'type', 'hemisphere',
    'home_competition_group', 'competition_group', 'cross_border_comp',
    'home_team_gender', 'home_team_type', 'home_team_level',
    'away_team_gender', 'away_team_type', 'away_team_level',
    'key_competition_name',
    'cross_competition_category',
    'home_pos_error_streak_adj',
    'home_neg_error_streak_adj',
    'away_pos_error_streak_adj',
    'away_neg_error_streak_adj',
    'last_game_distance_category',
    'features_z_score_cluster',
    'general_features_cluster',
    'expected_delta_category_1',
    'expected_delta_category_2',
    'expected_delta_category_3',
    'expected_delta_category_4',
    'expected_delta_adjusted_category_1',
    'expected_delta_adjusted_category_2',
    'expected_delta_adjusted_category_3',
    'expected_delta_adjusted_category_4',
    'expected_globalwinmargin_category_1',
    'expected_globalwinmargin_category_2',
    'expected_globalwinmargin_category_3',
    'expected_globalwinmargin_category_4',
    'expected_p1_winmargin_compspecific_category_1'
    ]

    # Add categorical variables
    all_features_df = pd.get_dummies(all_features_df, columns=categorical_features, drop_first=False)  # drop_first avoids multicollinearity





    return all_features_df




# Custom Median Absolute Error with Sign Penalty
def custom_median_loss(y_true, y_pred, lambda_sign=2.0):
    abs_errors = tf.abs(y_true - y_pred)
    median_abs_error = tfp.stats.percentile(abs_errors, 50.0, interpolation='midpoint')
    
    # Sign mismatch penalty
    sign_penalty = tf.where(tf.math.sign(y_true) != tf.math.sign(y_pred), 1.0, 0.0)

    return median_abs_error + (lambda_sign * tf.reduce_mean(sign_penalty))

# Custom Median Absolute Error with Scaled Sign Penalty
def custom_median_loss_scaled(y_true, y_pred, lambda_sign=2.0):
    abs_errors = tf.abs(y_true - y_pred)
    median_abs_error = tfp.stats.percentile(abs_errors, 50.0, interpolation='midpoint')

    # Sign mismatch penalty scaled by inverse of win margin (higher for small margins)
    sign_penalty = tf.where(tf.math.sign(y_true) != tf.math.sign(y_pred), 1.0 / (1.0 + tf.abs(y_true)), 0.0)

    return median_abs_error + (lambda_sign * tf.reduce_mean(sign_penalty))

class CustomLossWrapper(tf.keras.losses.Loss):
    def __init__(self, loss_function, lambda_sign=2.0, reduction=tf.keras.losses.Reduction.AUTO, name="custom_loss"):
        super().__init__(name=name, reduction=reduction)
        self.loss_function = loss_function
        self.lambda_sign = lambda_sign

    def call(self, y_true, y_pred):
        return self.loss_function(y_true, y_pred, self.lambda_sign)

    def get_config(self):  # Ensure it can be serialized
        config = super().get_config()
        config.update({
            "lambda_sign": self.lambda_sign,
            "loss_function": self.loss_function.__name__  # Store function name
        })
        return config

all_loss_functions = {'regression':
     [
        (tf.keras.losses.MeanSquaredError, {'reduction':'sum_over_batch_size'}),         # Suitable for continuous regression
        (tf.keras.losses.MeanAbsoluteError, {'reduction':'sum_over_batch_size'}),        # Good for less sensitive error measurement
        (tf.keras.losses.MeanAbsolutePercentageError, {'reduction':'sum_over_batch_size'}), # Useful when relative error matters
        (tf.keras.losses.MeanSquaredLogarithmicError, {'reduction':'sum_over_batch_size'}), # For handling smaller values in the dataset
        (tf.keras.losses.Huber, {'reduction':'sum_over_batch_size'}),                    # Combines advantages of MSE and MAE, robust to outliers
        (tf.keras.losses.LogCosh, {'reduction':'sum_over_batch_size'}),                    # Smooth approximation to MAE, often used in regression
        # (CustomLossWrapper, {'loss_function': custom_median_loss, 'lambda_sign': 1.0}),  # New loss function
        # (CustomLossWrapper, {'loss_function': custom_median_loss, 'lambda_sign': 1.5}),  # New loss function
        # (CustomLossWrapper, {'loss_function': custom_median_loss, 'lambda_sign': 2.0}),  # New loss function
        # (CustomLossWrapper, {'loss_function': custom_median_loss, 'lambda_sign': 2.5}),  # New loss function
        # (CustomLossWrapper, {'loss_function': custom_median_loss, 'lambda_sign': 3.0}),  # New loss function
        # (CustomLossWrapper, {'loss_function': custom_median_loss_scaled, 'lambda_sign': 1.0}),  # New loss function
        # (CustomLossWrapper, {'loss_function': custom_median_loss_scaled, 'lambda_sign': 1.5}),  # New loss function
        # (CustomLossWrapper, {'loss_function': custom_median_loss_scaled, 'lambda_sign': 2.0}),  # New loss function
        # (CustomLossWrapper, {'loss_function': custom_median_loss_scaled, 'lambda_sign': 2.5}),  # New loss function
        # (CustomLossWrapper, {'loss_function': custom_median_loss_scaled, 'lambda_sign': 3.0})  # New loss function
    ],
'binary_classification': [
#         (tf.keras.losses.CategoricalCrossentropy, {'reduction':'auto'}),
    #     tf.keras.losses.SparseCategoricalCrossentropy, {'reduction':'auto'}),
        (tf.keras.losses.BinaryCrossentropy, {'reduction':'sum_over_batch_size'}),
        (tf.keras.losses.KLDivergence, {'reduction':'sum_over_batch_size'}),
#         (tf.keras.losses.CosineSimilarity, {'reduction':'auto'}),
        (tf.keras.losses.Poisson, {'reduction':'sum_over_batch_size'}),
        (tf.keras.losses.Hinge, {'reduction':'sum_over_batch_size'}),
#         (tf.keras.losses.CategoricalHinge, {'reduction':'auto'})
    ]}


all_optimizers = [
    # Adam optimizer with default options
    (tf.keras.optimizers.Adam, {
        "beta_1": 0.9,        # Default
        "beta_2": 0.999,      # Default
        "epsilon": 1e-07,     # Default
        "amsgrad": False      # AMSGrad variant
    }),
    
    # Adam optimizer with more aggressive updates
    (tf.keras.optimizers.Adam, {
        "beta_1": 0.85,       # More responsive to recent gradients
        "beta_2": 0.95,       # Slightly less smooth estimates
        "epsilon": 1e-08,     # For higher precision
        "amsgrad": False
    }),
    
    # Adam optimizer with more stability
    (tf.keras.optimizers.Adam, {
        "beta_1": 0.99,       # Smoother updates
        "beta_2": 0.9995,     # More stable estimates
        "epsilon": 1e-06,     # For stability with small gradients
        "amsgrad": False
    }),
    
    # RMSprop optimizer with default options
    (tf.keras.optimizers.RMSprop, {
        "rho": 0.9,          # Default
        "momentum": 0.0,     # Default
        "epsilon": 1e-07,    # Default
        "centered": False   # Default
    }),
    
    # RMSprop optimizer with different settings
    (tf.keras.optimizers.RMSprop, {
        "rho": 0.8,          # Lower rho for more responsiveness
        "momentum": 0.1,     # Slightly higher momentum
        "epsilon": 1e-08,    # For higher precision
        "centered": True    # Centered RMSProp
    }),
    
    # SGD optimizer with momentum
    (tf.keras.optimizers.SGD, {
        "momentum": 0.9,     # Default
        "nesterov": True    # Default
    }),
    
    # SGD optimizer with different settings
    (tf.keras.optimizers.SGD, {
        "momentum": 0.5,     # Lower momentum
        "nesterov": False   # Standard SGD
    }),
    
    # Nadam optimizer
    (tf.keras.optimizers.Nadam, {
        "beta_1": 0.9,      # Default
        "beta_2": 0.999,    # Default
        "epsilon": 1e-07    # Default
    }),
    
    # Nadam optimizer with different settings
    (tf.keras.optimizers.Nadam, {
        "beta_1": 0.85,     # More responsive
        "beta_2": 0.95,     # Less smooth
        "epsilon": 1e-08    # For higher precision
    }),
    
    # Adadelta optimizer
    (tf.keras.optimizers.Adadelta, {
        "rho": 0.95,         # Default
        "epsilon": 1e-07     # Default
    }),
    
    # Adadelta optimizer with different settings
    (tf.keras.optimizers.Adadelta, {
        "rho": 0.9,          # Slightly lower rho
        "epsilon": 1e-08     # For higher precision
    }),
    
    # Adagrad optimizer
    (tf.keras.optimizers.Adagrad, {
        "initial_accumulator_value": 0.1, # Default
        "epsilon": 1e-07   # Default
    }),
    
    # Adagrad optimizer with different settings
    (tf.keras.optimizers.Adagrad, {
        "initial_accumulator_value": 0.01, # Lower initial accumulator value
        "epsilon": 1e-08   # For higher precision
    }),
    
    # Adamax optimizer
    (tf.keras.optimizers.Adamax, {
        "beta_1": 0.9,      # Default
        "beta_2": 0.999,    # Default
        "epsilon": 1e-07    # Default
    }),
    
    # Adamax optimizer with different settings
    (tf.keras.optimizers.Adamax, {
        "beta_1": 0.85,     # More responsive
        "beta_2": 0.95,     # Less smooth
        "epsilon": 1e-08    # For higher precision
    }),
    
    # Ftrl optimizer
    (tf.keras.optimizers.Ftrl, {
        "learning_rate_power": -0.5,     # Default
        "initial_accumulator_value": 0.1, # Default
        "l1_regularization_strength": 0.0, # Default
        "l2_regularization_strength": 0.0  # Default
    }),
    
    # Ftrl optimizer with different settings
    (tf.keras.optimizers.Ftrl, {
        "learning_rate_power": -0.5,     # Default
        "initial_accumulator_value": 0.05, # Lower initial accumulator value
        "l1_regularization_strength": 0.01, # Some regularization
        "l2_regularization_strength": 0.01  # Some regularization
    })
]



def iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict):
    
    # create empty list to store results
    results = []
    folds = []
    loop_num = 0
    

    print("class_weights_enabled: %s, stratification_enabled: %s, lr_schedule_enabled: %s, lr_schedule_strategies: %s, dropout_enabled: %s, dropout_rates: %s, early_stopping_metrics: %s, outlier_dicts: %s, activation_layers_list: %s, epochs: %s, loss_functions: %s, optimizers: %s, batch_sizes: %s, learning_rates: %s, pcas: %s, pca_thresholds: %s"%(len(class_weights_enabled), len(stratification_enabled), len(lr_schedule_enabled), len(lr_schedule_strategies), len(dropout_enabled), len(dropout_rates), len(early_stopping_metrics), len(outlier_dicts), len(activation_layers_list), len(epochs), len(loss_functions), len(optimizers), len(batch_sizes), len(learning_rates), len(pcas), len(pca_thresholds)))
    num_iterations = len(stratification_enabled) * len(lr_schedule_enabled) * len(lr_schedule_strategies) * len(dropout_enabled) * len(dropout_rates) * len(early_stopping_metrics) * len(class_weights_enabled) * len(outlier_dicts) * len(activation_layers_list) * len(epochs) * len(loss_functions) * len(optimizers) * len(batch_sizes) * len(learning_rates) * len(pcas) * len(pca_thresholds)

    indices = all_features_df.index

    # perform PCA on predictor variables (optional)
    for pca_on in pcas:

        print('pca-' + str(pca_on))

        if pca_on == False:
            pca_thresholds_checked = [None]
        else:
            pca_thresholds_checked = pca_thresholds

            
        for pca_threshold in pca_thresholds_checked:
            print('  ' + 'pca_thresholds_checked-' + str(pca_threshold))

            # loop through hyperparameters and train and test models
            for lr in learning_rates:
                print('  ' + 'lr-' + str(lr))

                for batch in batch_sizes:
                    print('        ' + 'batch-' + str(batch))

                    for opt in optimizers:
                        print('           ' + 'opt-' + str(opt))

                        for loss in loss_functions:
                            print('              ' + 'loss-' + str(loss))

                            for epoch in epochs:
                                print('                        ' + 'epoch-' + str(epoch))

                                for activation_layers in activation_layers_list:
                                    print('                              ' + 'activation_layer -' + str(activation_layers))

                                    for outlier_dict in outlier_dicts:
                                        print('                                   ' + 'outlier_dict -' + str(outlier_dict))

                                        for cwe in class_weights_enabled:
                                            print('                              ' + 'class_weights_enabled -' + str(cwe))

                                            for strat_enabled in stratification_enabled:
                                                print('                              ' + 'stratification_enabled -' + str(strat_enabled))


                                                for lr_schedule in lr_schedule_enabled:
                                                    print('                              ' + 'lr_schedule_enabled -' + str(lr_schedule))

                                                    # If we aren't using dropout rates then make sure we set the different rates to None so we aren't iterating the same thing
                                                    if lr_schedule == False:
                                                        lr_schedule_strategies_checked = [None]
                                                    else:
                                                        lr_schedule_strategies_checked = lr_schedule_strategies


                                                    for lss in lr_schedule_strategies_checked:
                                                        print('                              ' + 'lr_schedule_strategies -' + str(lss))


                                                        
                                                        for de in dropout_enabled:
                                                            print('                              ' + 'dropout_enabled -' + str(de))


                                                            # If we aren't using dropout rates then make sure we set the different rates to None so we aren't iterating the same thing
                                                            if de == False:
                                                                dropout_rates_checked = [None]
                                                            else:
                                                                dropout_rates_checked = dropout_rates


                                                            for de_rate in dropout_rates_checked:
                                                                print('                              ' + 'dropout_rates -' + str(de_rate))


                                                                for esm in early_stopping_metrics:
                                                                    print('                              ' + 'early_stopping_metric -' + str(esm))


                                                                    loop_num = loop_num + 1
                                                                    print(loop_num, num_iterations)


                                                                    result, feature_importances_df, fold_metrics = run_experiment(response_variable_name, transform_target, original_df, model_type, pca_on, pca_threshold, X, y, lr, batch, opt, loss, metrics, epoch, activation_layers, n_splits, cwe, outlier_dict = outlier_dict, stratification_enabled = strat_enabled, lr_schedule_enabled = lr_schedule, lr_schedule_strategy = lss, dropout_enabled = de, dropout_rate = de_rate, early_stopping_metric = esm, validation_dict = validation_dict)
                                                                    results.append(result)
                                                                    folds.append(fold_metrics)
    #                                                                 print(result)

                                                                    results_df = pd.DataFrame(results)
                                                                    folds_df = pd.DataFrame(folds)

                                                                    print('')
                                                                    print('-------------------------')
                                                                    print(results_df.sort_values(model_ass_val, ascending = model_ass_val_direction)[:10])

                                                                
    # Only exports the latest feature importances with model and scaler
    return results_df, folds_df, feature_importances_df


def run_experiment(
    model_type, pca_enabled, pca_threshold, X, y, learning_rate, batch_size, optimizer, 
    loss_function, metrics, epochs, activation_layers, n_splits, class_weights_enabled, 
    outlier_dict=None, patience=5, shap_samples=1000, stratification_enabled=False, 
    lr_schedule_enabled=False, lr_schedule_strategy=None, dropout_enabled=False, dropout_rate=0.5, 
    early_stopping_metric='val_loss', validation_dict = {}):
    
    global min_max_features
    global standard_scaler_features
    global categorical_features_to_use


    # Make sure outlier_flag isn't in X
    if 'outlier_flag' in X.columns:
        X.drop('outlier_flag', axis = 1, inplace = True)
        
    original_X = X.copy()

    # Convert X and y to numpy arrays if they are pandas DataFrames or Series
#     X = X.values if hasattr(X, 'values') else X
#     y = y.values if hasattr(y, 'values') else y
    

        
    # Set the scheduler if we are using one
    if lr_schedule_enabled and lr_schedule_strategy:
        lr_schedule_function = lr_schedule_strategy['method']


    # Handle Outlier Detection Configuration
    outlier_enabled = outlier_dict.get('enabled', False) if outlier_dict else False
    outlier_contamination = outlier_dict.get('contamination', 0.05) if outlier_enabled else None
    remove_outliers = outlier_dict.get('remove_outliers', False) if outlier_enabled else False

    
    # Step 1: Outlier Detection
    if outlier_enabled:
        outlier_detection = IsolationForest(contamination=outlier_contamination, random_state=42)
        outlier_flags = outlier_detection.fit_predict(X)  # -1 for outliers, 1 for inliers

        # Convert -1 to 1 (outlier) and 1 to 0 (inlier)
        outlier_flags = np.where(outlier_flags == -1, 1, 0)

        # Add the outlier flag as a new column in the DataFrame
        X['outlier_flag'] = outlier_flags
        
        # We aren't removing outliers but using them as a feature
        if remove_outliers == False:
            if 'outlier_flag' not in categorical_features_to_use:
                categorical_features_to_use.append('outlier_flag')
        else:
            # We are removing outliers later so make sure we aren't using it as a feature
            categorical_features_to_use = [x for x in categorical_features_to_use if x != 'outlier_flag']

            # Remove the outlier rows if applicable
            X = X[X['outlier_flag'] == 0].drop(columns=['outlier_flag'])
            y = y[X.index]

        
        # Outlier Summary
        total_outliers = np.sum(outlier_flags)
        total_samples = len(outlier_flags)
        percentage_outliers = (total_outliers / total_samples) * 100

        print(f"Outlier detection enabled.")
        print(f"Total samples: {total_samples}")
        print(f"Total outliers detected: {total_outliers} ({percentage_outliers:.2f}%)")

    else:
        outlier_detection = None
        categorical_features_to_use = [x for x in categorical_features_to_use if x != 'outlier_flag']
        
        
    preprocessor = ColumnTransformer(
    transformers=[
        ('minmax', MinMaxScaler(), min_max_features),                  
        ('std', StandardScaler(), standard_scaler_features),           
        ('cat', 'passthrough', categorical_features_to_use)  # This now includes 'outlier_flag'
    ]
    )


    # Track performance across folds
    fold_metrics = []
    feature_importances = []
    
    # Use KFold cross-validation with or without stratification
    if n_splits > 1:
        if stratification_enabled:
            kfold = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
        else:
            kfold = KFold(n_splits=n_splits, shuffle=True, random_state=42)
        splits = kfold.split(X, y)
    else:
        # Use a single train-test split
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y if stratification_enabled else None)
        splits = [(np.arange(len(X_train)), np.arange(len(X_test)) + len(X_train))]
    


    for fold_idx, (train_idx, test_idx) in enumerate(splits):
        print(f"Training fold {fold_idx + 1}...")

        # Split the data into training and testing sets
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]  # Ensure X is a DataFrame
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]  # Ensure y is a Series/DataFrame


        # X_train_transformed = preprocessor.fit_transform(X_train)
        # print(f"Original columns: {X_train.columns.shape}")
        # print(f"Transformed shape: {X_train_transformed.shape}")

        # missing_cols = set(X_train.columns) - set(pd.DataFrame(X_train_transformed).columns)
        # print(f"Missing Columns: {missing_cols}")

        X_train_scaled = preprocessor.fit_transform(X_train)
        feature_names = preprocessor.get_feature_names_out()

        X_train_scaled = pd.DataFrame(X_train_scaled, columns=feature_names, index=X_train.index).astype(np.float32)

        X_test_scaled = pd.DataFrame(preprocessor.transform(X_test), columns=feature_names, index=X_test.index).astype(np.float32)


        # Apply PCA if enabled
        if pca_enabled and pd.notna(pca_threshold):
            pca = PCA(n_components=pca_threshold)
            X_train_scaled = pca.fit_transform(X_train_scaled)
            print('X_pca shape: ' + str(X_train_scaled.shape))
            X_test_scaled = pca.transform(X_test_scaled)
            
        else:
            pca = None


    
        # Unpack optimizer and loss function
        optimizer_class, optimizer_params = optimizer
        optimizer_params['learning_rate'] = learning_rate
        optimizer_instance = optimizer_class(**optimizer_params)
        
        
#         if loss_function == 'custom_mae_sign_loss':
#             loss_function_to_use = custom_mae_loss
#         else:
#             loss_function_class, loss_function_params = loss_function
#             loss_function_to_use = loss_function_class(**loss_function_params)


        loss_function_class, loss_function_params = loss_function
        loss_function_to_use = loss_function_class(**loss_function_params)
        
        metrics_to_use = [m(**p) for m, p in metrics]

        # Learning Rate Scheduler (if enabled)            
        # Initialize the callbacks list
        callbacks = []

        # Learning Rate Scheduler (if enabled)
        if lr_schedule_enabled and lr_schedule_strategy:
            if isinstance(lr_schedule_function, ReduceLROnPlateau):
                # If lr_schedule_strategy is a ReduceLROnPlateau instance, add it directly
                callbacks.append(lr_schedule_function)
            elif callable(lr_schedule_function):
                # If lr_schedule_strategy is a callable function, use it with LearningRateScheduler
                lr_scheduler = LearningRateScheduler(lr_schedule_function)
                callbacks.append(lr_scheduler)
            else:
                print("Invalid lr_schedule_strategy provided. Must be callable or a ReduceLROnPlateau instance.")


        # Early Stopping with customizable metric
        early_stopping = EarlyStopping(monitor=early_stopping_metric, patience=patience, restore_best_weights=True)
        callbacks.append(early_stopping)

        # Initialize a sequential model
        model = tf.keras.Sequential()
        model.add(tf.keras.layers.Input(shape=X_train_scaled.shape[1]))

        # Hidden layers with Batch Normalization, Weight Initializers, and optional Dropout
        for a_layer in activation_layers:
            activation_function = a_layer['activation']
            initializer = HeNormal() if activation_function in ['relu', 'leaky_relu', 'elu'] else GlorotUniform()
            model.add(tf.keras.layers.Dense(
                a_layer['nodes'],
                activation=None,
                kernel_initializer=initializer,
                kernel_regularizer=tf.keras.regularizers.l2(a_layer['l2_reg']),
                activity_regularizer=tf.keras.regularizers.l1(a_layer['l1_reg'])
            ))
            model.add(tf.keras.layers.BatchNormalization())
            model.add(tf.keras.layers.Activation(activation_function))
            if dropout_enabled:
                model.add(tf.keras.layers.Dropout(dropout_rate))

        # Output layer
        if model_type == 'regression':
            model.add(tf.keras.layers.Dense(1, activation='linear'))
        else:
            model.add(tf.keras.layers.Dense(1, activation='sigmoid'))

        # Compile the model
        model.compile(optimizer=optimizer_instance, loss=loss_function_to_use, metrics=metrics_to_use)

        # Class Weights for Binary Classification
        class_weights = None
        class_weights_dict = None
        if model_type == 'binary_classification' and class_weights_enabled:

            # Compute class weights
            class_weights = compute_class_weight('balanced', classes=np.unique(y), y=y)

            # Convert the result into a dictionary for use in model.fit()
            class_weights_dict = dict(enumerate(class_weights))
            print("Class Weights:", class_weights_dict)
            


#             y_train_labels = np.argmax(y_train, axis=1) if y_train.ndim > 1 else y_train
#             class_weights = compute_class_weight('balanced', classes=np.unique(y_train_labels), y=y_train_labels)
#             class_weights = dict(enumerate(class_weights))

#         print(X_train_scaled.dtypes)

        # Train the model
        model.fit(
            X_train_scaled, y_train,
            batch_size=batch_size,
            epochs=epochs,
            verbose=0,
            validation_data=(X_test_scaled, y_test),
            callbacks=callbacks,
            class_weight=class_weights_dict
        )

        # Evaluate the model on the test set
        score = model.evaluate(X_test_scaled, y_test, verbose=0)
        y_pred = model.predict(X_test_scaled)

        # Handle metrics based on model type
        if model_type == 'regression':
            residuals = y_test - y_pred.flatten()
            mae_residual = np.mean(np.abs(residuals))
            median_ae_residual = np.median(np.abs(residuals))
            correct_sign_predictions = np.sum(np.sign(y_pred.flatten()) == np.sign(y_test.values.flatten()))
            sign_accuracy = correct_sign_predictions / len(y_test)

            fold_metrics.append({
                'score': score,
                'mean_absolute_error_residuals': mae_residual,
                'median_absolute_error_residuals': median_ae_residual,
                'sign_accuracy': sign_accuracy
            })            

            
        elif model_type == 'binary_classification':
            
#             y_pred_labels = np.argmax(y_pred, axis=1) if y_pred.ndim > 1 else y_pred
            y_pred_labels = (y_pred >= 0.5).astype(int)
            y_test_labels = np.argmax(y_test, axis=1) if y_test.ndim > 1 else y_test
            
            accuracy = accuracy_score(y_test_labels, y_pred_labels)

            # Compute confusion matrix and metrics
            cm = confusion_matrix(y_test_labels, y_pred_labels)
            TP, FN, FP, TN = cm[0, 0], cm[0, 1], cm[1, 0], cm[1, 1]

            # Calculate precision and recall
            precision = TP / (TP + FP) if (TP + FP) > 0 else 0
            recall = TP / (TP + FN) if (TP + FN) > 0 else 0
            f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
            TP_accuracy = TP / (TP + FP) if (TP + FP) > 0 else 0
            TN_accuracy = TN / (TN + FN) if (TN + FN) > 0 else 0

            fold_metrics.append({
                'accuracy': accuracy,
                'score': score,
                'precision': precision,
                'recall': recall,
                'f1_score': f1_score,
                'TP_accuracy': TP_accuracy,
                'TN_accuracy': TN_accuracy
            })

    # Summarize results
    ave_dict = {
        'avg_score': np.mean([fm['score'] for fm in fold_metrics], axis=0),
        'variance_score': np.var([fm['score'] for fm in fold_metrics], axis=0)
    }

    if model_type == 'regression':
        ave_dict.update({
            'mean_absolute_error_residuals_cv': np.mean([fm['mean_absolute_error_residuals'] for fm in fold_metrics]),
            'median_absolute_error_residuals_cv': np.mean([fm['median_absolute_error_residuals'] for fm in fold_metrics]),
            'avg_sign_accuracy': np.mean([fm['sign_accuracy'] for fm in fold_metrics])
        })

        
    elif model_type == 'binary_classification':
        ave_dict.update({
            'avg_accuracy': np.mean([fm['accuracy'] for fm in fold_metrics], axis=0),
            'avg_precision': np.mean([fm['precision'] for fm in fold_metrics]),
            'avg_recall': np.mean([fm['recall'] for fm in fold_metrics]),
            'avg_f1_score': np.mean([fm['f1_score'] for fm in fold_metrics]),
            'avg_TP_accuracy': np.mean([fm['TP_accuracy'] for fm in fold_metrics]),
            'avg_TN_accuracy': np.mean([fm['TN_accuracy'] for fm in fold_metrics]),
            'avg_TP_TN_accuracy': (np.mean([fm['TP_accuracy'] for fm in fold_metrics]) + np.mean([fm['TN_accuracy'] for fm in fold_metrics]))/2,
            'confusion_matrix': cm
        })

        
    feature_importances_df = pd.DataFrame(feature_importances, columns=[f'Feature_{i}' for i in range(X.shape[1])])

    
    # Use the current model to get the validation results
    validation_results, original_df_wProbabilities = get_validation_success(model_type, original_X, model, preprocessor, pca_enabled, pca_threshold, pca, outlier_dict, outlier_detection, validation_dict)
    
    
    results_dict = {
        'learning_rate': learning_rate,
        'batch_size': batch_size,
        'optimizer': optimizer,
        'optimizer_name': optimizer_class,
        'optimizer_params': optimizer_params,
        'loss_function': loss_function,
        'loss_function_class': loss_function_class,
        'loss_function_params': loss_function_params,
        'metrics': metrics,
        'epochs': epochs,
        'activation_layers': activation_layers,
        'pca_threshold': pca_threshold,
        'pca': pca_enabled,
        'pca_transform': pca,
        'n_splits': n_splits,
        'class_weights_enabled': class_weights_enabled,
        'outlier_dict': outlier_dict,
        'stratification_enabled': stratification_enabled,
        'lr_schedule_enabled': lr_schedule_enabled,
        'lr_schedule_strategy': lr_schedule_strategy,
        'dropout_enabled': dropout_enabled,
        'dropout_rate': dropout_rate,
        'early_stopping_metric': early_stopping_metric,
        'model': model,
#         'scaler': scaler,
        'preprocessor' : preprocessor,
        'outlier_detection': outlier_detection,
        'features': feature_names,
    }

    for key in ave_dict:
        results_dict[key] = ave_dict[key]

    for key in validation_results:
        results_dict[key] = validation_results[key]

    return results_dict, feature_importances_df, fold_metrics



def make_serializable(obj):
    if isinstance(obj, (int, float, str, bool, list, dict)):
        return obj
    else:
        return str(obj)  # Convert non-serializable objects to strings
    
    
    
all_metrics = {
    'regression': [
    (tf.keras.metrics.MeanSquaredError, {}),                # Measures the average of the squares of errors
    (tf.keras.metrics.MeanAbsoluteError, {}),               # Measures the average of absolute differences
    (tf.keras.metrics.MeanAbsolutePercentageError, {}),     # Measures the percentage error on average
    (tf.keras.metrics.RootMeanSquaredError, {}),            # Square root of MSE, more interpretable in original units
    (tf.keras.metrics.CosineSimilarity, {}),                # Measures similarity between actual and predicted vectors
    (tf.keras.metrics.LogCoshError, {}),                    # Robust to outliers, smooth approximation of MAE
    # (tfa.metrics.RSquare, {})                               # Coefficient of determination (R²), measures explained variance
],
    'binary_classification':
[    
#                (tf.keras.metrics.CategoricalAccuracy, {}), 
               (tf.keras.metrics.AUC, {}),
               (tf.keras.metrics.BinaryAccuracy, {}),
               (tf.keras.metrics.Precision, {}),
               (tf.keras.metrics.Recall, {}),
#                (tfa.metrics.F1Score, {'num_classes':num_classes, 'average':'macro'}), 
               (tf.keras.metrics.TruePositives, {}),
               (tf.keras.metrics.FalsePositives, {}),
               (tf.keras.metrics.TrueNegatives, {}),
               (tf.keras.metrics.FalseNegatives, {}),
               (tf.keras.metrics.BinaryCrossentropy, {}),
#                (tfa.metrics.MatthewsCorrelationCoefficient, {'num_classes':2})
              ]}




all_activations = [
    'relu',          # Common choice for hidden layers
    'tanh',          # Useful if data has both positive and negative values
    'elu',           # More robust variant of ReLU
    tf.keras.activations.swish,  # Self-gated activation
    'softplus',      # Smooth approximation of ReLU
    'softsign',      # Similar to tanh, squashes inputs between -1 and 1
    tf.keras.activations.gelu,   # Gaussian Error Linear Unit (GELU)
    'linear'         # Typically used for the output layer in regression
]


model_keys_to_export = [
'learning_rate',
'batch_size',
'optimizer',
'loss_function',
'epochs',
'activation_layers',
'pca_threshold',
'pca',
'n_splits',
'class_weights_enabled',
'outlier_dict',
'stratification_enabled',
'lr_schedule_enabled',
'lr_schedule_strategy',
'dropout_enabled',
'dropout_rate',
'early_stopping_metric',
]


all_outlier_dicts = [{'enabled':False, 'contamination':0.05, 'remove_outliers':True},
                     {'enabled':True, 'contamination':0.05, 'remove_outliers':True},
                     {'enabled':True, 'contamination':0.05, 'remove_outliers':False},
                     {'enabled':True, 'contamination':0.02, 'remove_outliers':True}
                    ]


all_early_stopping_metrics_bc = [
#     'val_loss',            # Validation Loss
#     'val_binary_accuracy', # Validation Binary Accuracy (for binary classification)
    'loss',
    'val_binary_crossentropy',
    'val_MatthewsCorrelationCoefficient'
]

early_stopping_min_metrics_bc = [
    'val_loss', 'loss', 'val_binary_crossentropy', 
    'val_mean_absolute_error', 'val_mean_squared_error',
    'val_mean_squared_logarithmic_error', 'val_root_mean_squared_error'
]

# Metrics that should be maximized
early_stopping_max_metrics_bc = [
    'val_binary_accuracy', 'val_MatthewsCorrelationCoefficient', 'val_r2'
]


all_early_stopping_metrics_reg = [
'loss',              # General validation loss, can be set to MSE, MAE, etc.
'val_loss',              # General validation loss, can be set to MSE, MAE, etc.
'val_mean_absolute_error',   # Validation Mean Absolute Error
'val_mean_squared_error',    # Validation Mean Squared Error
#     'val_mean_squared_logarithmic_error', # Validation MSLE (if target scale varies greatly)
'val_root_mean_squared_error', # Validation RMSE
#     'val_r2'                    # Validation R-Squared (if available in your framework)
]



def get_additional_layers():
    
    additional_layers = []

    for nodes in all_num_nodes:
        for activation in all_activations:
            for l2 in all_l2_regs:
                for l1 in all_l1_regs:

                    additional_layer = dict()
                    additional_layer['nodes'] = nodes
                    additional_layer['activation'] = activation
                    additional_layer['l2_reg'] = l2
                    additional_layer['l1_reg'] = l1

                    additional_layers.append(additional_layer)

    return additional_layers




def get_lr_schedule_strategies():

    # Define each learning rate schedule with multiple variations

    # 1. Exponential Decay: Trying different decay rates
    def exponential_decay_95(epoch, lr): return lr * 0.95  # 5% reduction per epoch
    def exponential_decay_90(epoch, lr): return lr * 0.90  # 10% reduction per epoch
    def exponential_decay_85(epoch, lr): return lr * 0.85  # 15% reduction per epoch
    def exponential_decay_80(epoch, lr): return lr * 0.80  # 20% reduction per epoch

    # 2. Step Decay: Varying the drop rate and interval
    def step_decay_50_10(epoch, lr):
        drop_rate = 0.5
        drop_every = 10
        return lr * drop_rate if epoch % drop_every == 0 else lr

    def step_decay_30_10(epoch, lr):
        drop_rate = 0.3
        drop_every = 10
        return lr * drop_rate if epoch % drop_every == 0 else lr

    def step_decay_50_5(epoch, lr):
        drop_rate = 0.5
        drop_every = 5
        return lr * drop_rate if epoch % drop_every == 0 else lr

    def step_decay_70_10(epoch, lr):
        drop_rate = 0.7
        drop_every = 10
        return lr * drop_rate if epoch % drop_every == 0 else lr

    # 3. Time-Based Decay: Trying different decay rates
    def time_based_decay_1e4(epoch, lr): return lr / (1 + 1e-4 * epoch)
    def time_based_decay_1e5(epoch, lr): return lr / (1 + 1e-5 * epoch)
    def time_based_decay_5e4(epoch, lr): return lr / (1 + 5e-4 * epoch)
    def time_based_decay_2e4(epoch, lr): return lr / (1 + 2e-4 * epoch)

    # 4. Cosine Annealing: Different cycles and minimum learning rates
    def cosine_annealing_30(epoch, lr):
        min_lr = 1e-5
        max_lr = lr
        return min_lr + 0.5 * (max_lr - min_lr) * (1 + np.cos(np.pi * epoch / 30))

    def cosine_annealing_50(epoch, lr):
        min_lr = 1e-5
        max_lr = lr
        return min_lr + 0.5 * (max_lr - min_lr) * (1 + np.cos(np.pi * epoch / 50))

    def cosine_annealing_10(epoch, lr):
        min_lr = 1e-5
        max_lr = lr
        return min_lr + 0.5 * (max_lr - min_lr) * (1 + np.cos(np.pi * epoch / 10))

    def cosine_annealing_20(epoch, lr):
        min_lr = 1e-5
        max_lr = lr
        return min_lr + 0.5 * (max_lr - min_lr) * (1 + np.cos(np.pi * epoch / 20))

    # 5. Reduce on Plateau: Trying different patience and reduction factors
    reduce_lr_50_5 = tf.keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=5, min_lr=1e-5)
    reduce_lr_30_5 = tf.keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.3, patience=5, min_lr=1e-5)
    reduce_lr_50_10 = tf.keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=10, min_lr=1e-5)
    reduce_lr_70_5 = tf.keras.callbacks.ReduceLROnPlateau(monitor='val_loss', factor=0.7, patience=5, min_lr=1e-5)

    # Collect all learning rate schedules with variations
    all_lr_schedule_strategies = [
        # Exponential Decay variations
        {'method': exponential_decay_95, 'description': "Exponential Decay 5%"},
        {'method': exponential_decay_90, 'description': "Exponential Decay 10%"},
        {'method': exponential_decay_85, 'description': "Exponential Decay 15%"},
        {'method': exponential_decay_80, 'description': "Exponential Decay 20%"},

        # Step Decay variations
        {'method': step_decay_50_10, 'description': "Step Decay 50% every 10 epochs"},
        {'method': step_decay_30_10, 'description': "Step Decay 30% every 10 epochs"},
        {'method': step_decay_50_5, 'description': "Step Decay 50% every 5 epochs"},
        {'method': step_decay_70_10, 'description': "Step Decay 70% every 10 epochs"},

        # Time-Based Decay variations
        {'method': time_based_decay_1e4, 'description': "Time-Based Decay 1e-4"},
        {'method': time_based_decay_1e5, 'description': "Time-Based Decay 1e-5"},
        {'method': time_based_decay_5e4, 'description': "Time-Based Decay 5e-4"},
        {'method': time_based_decay_2e4, 'description': "Time-Based Decay 2e-4"},

        # Cosine Annealing variations
        {'method': cosine_annealing_30, 'description': "Cosine Annealing with 30 epochs cycle"},
        {'method': cosine_annealing_50, 'description': "Cosine Annealing with 50 epochs cycle"},
        {'method': cosine_annealing_10, 'description': "Cosine Annealing with 10 epochs cycle"},
        {'method': cosine_annealing_20, 'description': "Cosine Annealing with 20 epochs cycle"},

        # Reduce on Plateau variations
        {'method': reduce_lr_50_5, 'description': "Reduce on Plateau 50%, patience 5"},
        {'method': reduce_lr_30_5, 'description': "Reduce on Plateau 30%, patience 5"},
        {'method': reduce_lr_50_10, 'description': "Reduce on Plateau 50%, patience 10"},
        {'method': reduce_lr_70_5, 'description': "Reduce on Plateau 70%, patience 5"}
    ]

    
    return all_lr_schedule_strategies


all_lr_schedule_strategies = get_lr_schedule_strategies()


def find_optimal_general_parameters(model_name, response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, optimal_iterations, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, model_layer, validation_dict):
    
    optimal_parameters_df_name = str(model_name) + '_optimal_parameters_df.csv'
    
    if os.path.exists(optimal_parameters_df_name):
        with open(optimal_parameters_df_name, 'r') as f:
            optimal_parameters_df = f.read()
            print(f"{optimal_parameters_df_name} exists")
        optimal_parameters_df = pd.read_csv(optimal_parameters_df_name)

    
    else:
        optimal_parameters_df = pd.DataFrame(columns = ['model_layer', 'fine_tuning', 'iteration_num'])

    for loop_num in range(0, optimal_iterations):
        
        print("'''''''' Getting optimal parameters ''''''''")
        print("''''''''", optimal_iterations)
        print('')

        
        print("'''''''' pcas")
        fine_tuning = 'pca'
        results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
        
        if len(results_df) == 0:
            
            pcas = all_pcas
            pca_thresholds = all_pca_thresholds
            
            results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)
            results_df['model_layer'] = model_layer
            results_df['fine_tuning'] = fine_tuning
            results_df['iteration_num'] = loop_num
            optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)


            pcas = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['pca']]
            pca_thresholds = [float(results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['pca_threshold'])]

            print('pcas Selected - ', pcas)
            print('pca_thresholds Selected - ', pca_thresholds)
            print('')
        

        print("'''''''' LR & BS")
        fine_tuning = 'lr_bs'
        results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
        
        if len(results_df) == 0:

            learning_rates = all_learning_rates
            batch_sizes = all_batch_sizes
            
            results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)
            results_df['model_layer'] = model_layer
            results_df['fine_tuning'] = fine_tuning
            results_df['iteration_num'] = loop_num
            optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)

        top_model = results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]
        batch_sizes = [int(top_model['batch_size'])]
        learning_rates = [float(top_model['learning_rate'])]
        
        print('BS Selected - ', batch_sizes)
        print('LR Selected - ', learning_rates)
        print('')


        print("'''''''' optimizers")
        fine_tuning = 'optimizers'
        results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
        
        if len(results_df) == 0:

            optimizers = all_optimizers

            results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)
            results_df['model_layer'] = model_layer
            results_df['fine_tuning'] = fine_tuning
            results_df['iteration_num'] = loop_num
            optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)
            optimizers = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['optimizer']]

        else:
            
            optimizers = results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['optimizer']
            optimizers = replace_df_objects_with_functions(optimizers)
            optimizers = optimizers.replace(': nan,', ': None,')
            optimizers = [eval(optimizers)]


        print('optimizers Selected - ', optimizers)
        print('')

            
        print("'''''''' loss_functions")
        fine_tuning = 'loss_functions'
        results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
        
        if len(results_df) == 0:

            # Now add best 2 loss functions
            if model_type == 'regression':
                loss_functions = all_loss_functions['regression']
            elif model_type == 'binary_classification':
                loss_functions = all_loss_functions['binary_classification']

            
            results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)
            results_df['model_layer'] = model_layer
            results_df['fine_tuning'] = fine_tuning
            results_df['iteration_num'] = loop_num
            optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)
            loss_functions = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['loss_function']]

        else:
            
            loss_functions = results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['loss_function']
            loss_functions = replace_df_objects_with_functions(loss_functions)
            loss_functions = loss_functions.replace(': nan,', ': None,')
            loss_functions = [eval(loss_functions)]

        print('loss_functions Selected - ', loss_functions)
        print('')

            

        print("'''''''' class_weights_enabled")
        fine_tuning = 'class_weights_enabled'
        results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
        
        if len(results_df) == 0:

            class_weights_enabled = [True, False]
            
            results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)
            results_df['model_layer'] = model_layer
            results_df['fine_tuning'] = fine_tuning
            results_df['iteration_num'] = loop_num
            optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)

        class_weights_enabled = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['class_weights_enabled']]
        print('class_weights_enabled Selected - ', class_weights_enabled)
        print('')


        
        print("'''''''' stratification_enabled")
        fine_tuning = 'stratification_enabled'
        
        if model_type == 'binary_classification':
    
            results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
            
            if len(results_df) == 0:
    
                    
                    stratification_enabled = [True, False]
                    
                    results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)
                    results_df['model_layer'] = model_layer
                    results_df['fine_tuning'] = fine_tuning
                    results_df['iteration_num'] = loop_num
                    optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)
    
            stratification_enabled = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['stratification_enabled']]
            print('stratification_enabled Selected - ', stratification_enabled)
            print('')
    
            
        
        print("'''''''' dropout_enabled")
        fine_tuning = 'dropout_enabled'
        results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
        
        if len(results_df) == 0:

            dropout_enabled = [False, True]
            dropout_rates = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.9, 0.95]
            print('herre')
            results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)
            results_df['model_layer'] = model_layer
            results_df['fine_tuning'] = fine_tuning
            results_df['iteration_num'] = loop_num
            optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)

        dropout_enabled = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['dropout_enabled']]
        dropout_rates = [float(results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['dropout_rate'])]
        print('dropout_enabled Selected - ', dropout_enabled)
        print('dropout_rates Selected - ', dropout_rates)
        print('')

        
        
        print("'''''''' early_stopping_metrics")
        fine_tuning = 'early_stopping_metrics'
        results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
        
        if len(results_df) == 0:

            if model_type == 'regression':
                early_stopping_metrics = all_early_stopping_metrics_reg
            elif model_type == 'binary_classification':
                early_stopping_metrics = all_early_stopping_metrics_bc


            results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)
            results_df['model_layer'] = model_layer
            results_df['fine_tuning'] = fine_tuning
            results_df['iteration_num'] = loop_num
            optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)
            
        early_stopping_metrics = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['early_stopping_metric']]
        print('early_stopping_metrics Selected - ', early_stopping_metrics)
        print('')


        
        print("'''''''' outlier_dicts")
        fine_tuning = 'outlier_dicts'
        results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
        
        if len(results_df) == 0:

            outlier_dicts = all_outlier_dicts

            results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)
            results_df['model_layer'] = model_layer
            results_df['fine_tuning'] = fine_tuning
            results_df['iteration_num'] = loop_num
            optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)

        outlier_dicts = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['outlier_dict']]
        print('outlier_dicts Selected - ', outlier_dicts)
        print('')


        
        model_to_use = optimal_parameters_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0].to_dict()

        
        # Use just the top metrics
        print("'''''''' lr_schedule_enabled")
        fine_tuning = 'lr_schedule'
        results_df = optimal_parameters_df[ (optimal_parameters_df['model_layer'] == model_layer) & (optimal_parameters_df['iteration_num'] == loop_num) & (optimal_parameters_df['fine_tuning'] == fine_tuning) ]
        
        if len(results_df) == 0:

            lr_schedule_enabled = [True, False]
            lr_schedule_strategies = all_lr_schedule_strategies

#             results_df, folds_df, feature_importances_df = iterate_through_model_variations(model_ass_val, model_ass_val_direction, [model_to_use['pca']], [model_to_use['pca_threshold']], [model_to_use['learning_rate']], [model_to_use['batch_size']], [model_to_use['optimizer']], [model_to_use['loss_function']], metrics, epochs, [model_to_use['activation_layers']], n_splits, [model_to_use['class_weights_enabled']], [model_to_use['outlier_dict']], [model_to_use['stratification_enabled']], lr_schedule_enabled, lr_schedule_strategies, [model_to_use['dropout_enabled']], [model_to_use['dropout_rate']], [model_to_use['early_stopping_metric']], validation_dict = validation_dict)
            results_df, folds_df, feature_importances_df = iterate_through_model_variations(response_variable_name, transform_target, original_df, all_features_df, model_type, X, y, model_ass_val, model_ass_val_direction, pcas, pca_thresholds, learning_rates, batch_sizes, optimizers, loss_functions, metrics, epochs, activation_layers_list, n_splits, class_weights_enabled, outlier_dicts, stratification_enabled, lr_schedule_enabled, lr_schedule_strategies, dropout_enabled, dropout_rates, early_stopping_metrics, validation_dict = validation_dict)

            results_df['model_layer'] = model_layer
            results_df['fine_tuning'] = fine_tuning
            results_df['iteration_num'] = loop_num
            optimal_parameters_df = pd.concat([optimal_parameters_df, results_df], ignore_index = True)
            optimal_parameters_df.to_csv(optimal_parameters_df_name, index = False)

            lr_schedule_enabled = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['lr_schedule_enabled']]
            lr_schedule_strategies = [results_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0]['lr_schedule_strategy']]
            print('lr_schedule_enabled Selected - ', lr_schedule_enabled)
            print('lr_schedule_strategies Selected - ', lr_schedule_strategies)
            print('')
            
            # We don't need to set the lr_schedule_strategies because we are just taking the top model below
            
        top_model = optimal_parameters_df.sort_values(model_ass_val, ascending = model_ass_val_direction).iloc[0].to_dict()

        
        
        return top_model, optimal_parameters_df
    
    
    
    
def get_validation_success(response_variable_name, transform_target, model_type, original_df, original_X, model, scaler_to_use, pca_enabled, pca_threshold, pca_transform, outlier_dict, outlier_detection, validation_dict):
        
    validation_range_column = validation_dict['validation_range_column']
    validation_start_range = validation_dict['validation_start_range']
    validation_end_range = validation_dict['validation_end_range']
    train_start_range = validation_dict['train_start_range']
    validation_comparison_column = validation_dict['validation_comparison_column']
    validation_success_column = validation_dict['validation_success_column']
    validation_agg_type = validation_dict['validation_agg_type']
    
    model_success_direction_asc = validation_dict['model_success_direction_asc']
        
    
    original_X_columns = original_X.columns
    original_df_to_use = original_df.copy()
    
    
    validation_results = dict()
    
    # Get the validation data
    test_data = original_df_to_use[original_X_columns]

    
    # Apply PCA if enabled
    if pca_enabled and pd.notna(pca_threshold):
        test_data = pca_transform.transform(test_data)
        
        
    # Need to see if we add an outlier column before making out predictions
    outlier_enabled = outlier_dict.get('enabled', False) if outlier_dict else False
    remove_outliers = outlier_dict.get('remove_outliers', False) if outlier_enabled else False

    # Outlier Detection
    if outlier_enabled & (remove_outliers == False):
        outlier_flags = outlier_detection.predict(test_data)  # -1 for outliers, 1 for inliers
        outlier_flags = np.where(outlier_flags == -1, 1, 0)  # 1 is an outlier, 0 is an inlier
        test_data = np.column_stack((test_data, outlier_flags))  # Append the outlier flag as a new feature


    # Eemember to scale
    test_data = scaler_to_use.transform(test_data)
    
    if model_type == 'binary_classification':

        # Now predict witht the current model
        original_df_to_use['prediction'] = model.predict(test_data)

        original_df_to_use['predicted_winner'] = original_df_to_use['prediction'].apply(lambda x: 1 if x >= 0.5 else 0)
        original_df_to_use['predicted_success'] = original_df_to_use[[response_variable_name, 'predicted_winner']].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else 1 if x[0] == x[1] else 0, axis = 1)

        # Train Results
        validation_df = original_df_to_use.loc[original_X.index]
        validation_results['validation_training_success'] = validation_df['predicted_success'].mean()
        
        if validation_comparison_column is not None:
            
            if model_success_direction_asc == True:
                
                if validation_agg_type == 'mean':
                    validation_results['validation_training_improvement'] = (validation_df[validation_comparison_column].mean() - validation_df[validation_success_column].mean()) / validation_df[validation_comparison_column].mean()
                elif validation_agg_type == 'median':
                    validation_results['validation_training_improvement'] = (validation_df[validation_comparison_column].median() - validation_df[validation_success_column].median()) / validation_df[validation_comparison_column].median()

            else:
                
                if validation_agg_type == 'mean':
                    validation_results['validation_training_improvement'] = (validation_df[validation_success_column].mean() - validation_df[validation_comparison_column].mean()) / validation_df[validation_comparison_column].mean()
                elif validation_agg_type == 'median':
                    validation_results['validation_training_improvement'] = (validation_df[validation_success_column].median() - validation_df[validation_comparison_column].median()) / validation_df[validation_comparison_column].median()

            print('Training: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_training_improvement'], validation_df[validation_comparison_column].mean(), validation_df[validation_success_column].mean()))

        else:
            validation_results['validation_training_improvement'] = None
        

        # Test Results
        
        validation_df = original_df_to_use[(original_df_to_use[validation_range_column] >= (validation_start_range)) & (original_df_to_use[validation_range_column] < (validation_end_range))]
        validation_results['validation_test_success'] = validation_df['predicted_success'].mean()

        if validation_comparison_column is not None:

            if model_success_direction_asc == True:
                
                if validation_agg_type == 'mean':
                    validation_results['validation_test_improvement'] = (validation_df[validation_comparison_column].mean() - validation_df[validation_success_column].mean()) / validation_df[validation_comparison_column].mean()
                elif validation_agg_type == 'median':
                    validation_results['validation_test_improvement'] = (validation_df[validation_comparison_column].median() - validation_df[validation_success_column].median()) / validation_df[validation_comparison_column].median()

            else:
                
                if validation_agg_type == 'mean':
                    validation_results['validation_test_improvement'] = (validation_df[validation_success_column].mean() - validation_df[validation_comparison_column].mean()) / validation_df[validation_comparison_column].mean()
                elif validation_agg_type == 'median':
                    validation_results['validation_test_improvement'] = (validation_df[validation_success_column].median() - validation_df[validation_comparison_column].median()) / validation_df[validation_comparison_column].median()

            validation_results['validation_avg_train_test_improvement'] = (validation_results['validation_test_improvement'] + validation_results['validation_training_improvement']) / 2
            validation_results['validation_train_test_harmonic_mean'] = 2 * ((validation_results['validation_test_improvement'] * validation_results['validation_training_improvement']) / (validation_results['validation_test_improvement'] + validation_results['validation_training_improvement']))
            if (validation_results['validation_test_improvement'] < 0) & (validation_results['validation_training_improvement'] < 0):
                validation_results['validation_train_test_geometric_mean'] = None
            else:
                validation_results['validation_train_test_geometric_mean'] = np.sqrt((validation_results['validation_test_improvement'] * validation_results['validation_training_improvement']))
        else:
            validation_results['validation_test_improvement'] = None

        print('Test: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_test_improvement'], validation_df[validation_comparison_column].mean(), validation_df[validation_success_column].mean()))

    elif model_type == 'regression':
        
        original_df_to_use['prediction'] = model.predict(test_data)
        
        # If transformed then transform back
        if transform_target == 'tan':
            temp_df = original_df_to_use.loc[original_X.index]
            # Original mean and standard deviation of the win margin
            mean = temp_df[response_variable_name].mean()
            std = temp_df[response_variable_name].std()

            # Assuming `predicted_transformed_margin` contains your model's predictions
            predicted_normalized_margin = np.arctanh( original_df_to_use['prediction'])
            print(predicted_normalized_margin)

            # De-normalize to get the original win margin scale
            original_df_to_use['prediction'] = (predicted_normalized_margin * std) + mean

        if transform_target == 'sqrt':

            original_df_to_use['prediction_sign'] = original_df_to_use['prediction'].apply(lambda x: -1 if x < 0 else 1)
            original_df_to_use['prediction'] = original_df_to_use['prediction'] * original_df_to_use['prediction'] * original_df_to_use['prediction_sign']
#             original_df_to_use['prediction'] = original_df_to_use[['prediction', 'prediction_sign']].apply(lambda x: np.power(abs(x[0]), 4/3) * x[1], axis = 1)
        
        elif transform_target == 'power_34':

            original_df_to_use['prediction_sign'] = original_df_to_use['prediction'].apply(lambda x: -1 if x < 0 else 1)
            original_df_to_use['prediction'] = original_df_to_use[['prediction', 'prediction_sign']].apply(lambda x: np.power(abs(x[0]), 4/3) * x[1], axis = 1)




#         print(mean, std, original_df_to_use[['prediction', response_variable_name]])
            
        original_df_to_use['prediction_error'] = original_df_to_use[['prediction', response_variable_name]].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else x[0] - x[1], axis = 1)
        original_df_to_use['prediction_error_abs'] = original_df_to_use['prediction_error'].apply(lambda x: None if pd.isna(x) else abs(x))
        original_df_to_use['prediction_sign_success'] = original_df_to_use[['prediction', response_variable_name]].apply(lambda x: None if pd.isna(x[0]) | pd.isna(x[1]) else 1 if ((x[0] > 0) & (x[1] > 0)) | ((x[0] < 0) & (x[1] < 0)) else 0, axis = 1)

        # Training Results
        validation_df = original_df_to_use.loc[original_X.index]

        validation_results['validation_train_error_mean'] = validation_df['prediction_error'].mean()
        validation_results['validation_train_error_median'] = validation_df['prediction_error'].median()
        validation_results['validation_train_error_std'] = validation_df['prediction_error'].std()
        
        validation_results['validation_train_abserror_mean'] = validation_df['prediction_error_abs'].mean()
        validation_results['validation_train_abserror_median'] = validation_df['prediction_error_abs'].median()
        validation_results['validation_train_abserror_std'] = validation_df['prediction_error_abs'].std()
        
        validation_results['validation_training_success'] = validation_df['prediction_sign_success'].mean()
        
        if validation_comparison_column is not None:
            
            if validation_comparison_column == 'base':
                if validation_agg_type == 'mean':
                    validation_results['validation_training_improvement'] = validation_df[validation_success_column].mean()                
                    print('Training: Improvement: %s, New: %s'%(validation_results['validation_training_improvement'], validation_df[validation_success_column].mean()))

                elif validation_agg_type == 'median':
                    validation_results['validation_training_improvement'] = validation_df[validation_success_column].median()
                    print('Training: Improvement: %s, New: %s'%(validation_results['validation_training_improvement'], validation_df[validation_success_column].median()))


            else:

                if model_success_direction_asc == True:

                    if validation_agg_type == 'mean':
                        validation_results['validation_training_improvement'] = (validation_df[validation_comparison_column].mean() - validation_df[validation_success_column].mean()) / validation_df[validation_comparison_column].mean()
                        print('Training: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_training_improvement'], validation_df[validation_comparison_column].mean(), validation_df[validation_success_column].mean()))

                    elif validation_agg_type == 'median':
                        validation_results['validation_training_improvement'] = (validation_df[validation_comparison_column].median() - validation_df[validation_success_column].median()) / validation_df[validation_comparison_column].median()
                        print('Training: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_training_improvement'], validation_df[validation_comparison_column].median(), validation_df[validation_success_column].median()))

                else:

                    if validation_agg_type == 'mean':
                        
                        validation_results['validation_training_improvement'] = (validation_df[validation_success_column].mean() - validation_df[validation_comparison_column].mean()) / validation_df[validation_comparison_column].mean()
                        print('Training: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_training_improvement'], validation_df[validation_comparison_column].mean(), validation_df[validation_success_column].mean()))

                    elif validation_agg_type == 'median':
                        validation_results['validation_training_improvement'] = (validation_df[validation_success_column].median() - validation_df[validation_comparison_column].median()) / validation_df[validation_comparison_column].median()
                        print('Training: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_training_improvement'], validation_df[validation_comparison_column].median(), validation_df[validation_success_column].median()))


        else:
            validation_results['validation_training_improvement'] = None


        
        # Test Results
        validation_df = original_df_to_use[(original_df_to_use[validation_range_column] >= (validation_start_range)) & (original_df_to_use[validation_range_column] < (validation_end_range))]

        validation_results['validation_test_error_mean'] = validation_df['prediction_error'].mean()
        validation_results['validation_test_error_median'] = validation_df['prediction_error'].median()
        validation_results['validation_test_error_std'] = validation_df['prediction_error'].std()
        
        validation_results['validation_test_abserror_mean'] = validation_df['prediction_error_abs'].mean()
        validation_results['validation_test_abserror_median'] = validation_df['prediction_error_abs'].median()
        validation_results['validation_test_abserror_std'] = validation_df['prediction_error_abs'].std()
        
        validation_results['validation_test_success'] = validation_df['prediction_sign_success'].mean()

        if validation_comparison_column is not None:
            
            if validation_comparison_column == 'base':
                if validation_agg_type == 'mean':
                    validation_results['validation_test_improvement'] = validation_df[validation_success_column].mean()
                    print('Test: Improvement: %s, New: %s'%(validation_results['validation_test_improvement'], validation_df[validation_success_column].mean()))

                elif validation_agg_type == 'median':
                    validation_results['validation_test_improvement'] = validation_df[validation_success_column].median()
                    print('Test: Improvement: %s, New: %s'%(validation_results['validation_test_improvement'], validation_df[validation_success_column].median()))

            else:


                if model_success_direction_asc == True:

                    if validation_agg_type == 'mean':
                        validation_results['validation_test_improvement'] = (validation_df[validation_comparison_column].mean() - validation_df[validation_success_column].mean()) / validation_df[validation_comparison_column].mean()
                        print('Test: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_test_improvement'], validation_df[validation_comparison_column].mean(), validation_df[validation_success_column].mean()))

                    elif validation_agg_type == 'median':
                        validation_results['validation_test_improvement'] = (validation_df[validation_comparison_column].median() - validation_df[validation_success_column].median()) / validation_df[validation_comparison_column].median()
                        print('Test: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_test_improvement'], validation_df[validation_comparison_column].median(), validation_df[validation_success_column].median()))

                else:

                    if validation_agg_type == 'mean':
                        validation_results['validation_test_improvement'] = (validation_df[validation_success_column].mean() - validation_df[validation_comparison_column].mean()) / validation_df[validation_comparison_column].mean()
                        print('Test: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_test_improvement'], validation_df[validation_comparison_column].mean(), validation_df[validation_success_column].mean()))

                    elif validation_agg_type == 'median':
                        validation_results['validation_test_improvement'] = (validation_df[validation_success_column].median() - validation_df[validation_comparison_column].median()) / validation_df[validation_comparison_column].median()
                        print('Test: Improvement: %s, Original: %s, New: %s'%(validation_results['validation_test_improvement'], validation_df[validation_comparison_column].median(), validation_df[validation_success_column].median()))


                    
            validation_results['validation_avg_train_test_improvement'] = (validation_results['validation_test_improvement'] + validation_results['validation_training_improvement']) / 2
            validation_results['validation_train_test_harmonic_mean'] = 2 * ((validation_results['validation_test_improvement'] * validation_results['validation_training_improvement']) / (validation_results['validation_test_improvement'] + validation_results['validation_training_improvement']))
            
            if (validation_results['validation_test_improvement'] < 0) & (validation_results['validation_training_improvement'] < 0):
                validation_results['validation_train_test_geometric_mean'] = None
            else:
                validation_results['validation_train_test_geometric_mean'] = np.sqrt((validation_results['validation_test_improvement'] * validation_results['validation_training_improvement']))
        

        else:
            validation_results['validation_test_improvement'] = None

        


    return validation_results, original_df_to_use['prediction']






def save_model_and_attributes(folder_name, model_name, top_model):

    temp_model_name = os.path.join(folder_name, model_name)


    model = top_model['model']
#     scaler_to_use = top_model['scaler']
    preprocessor = top_model['preprocessor']

    outlier_model = top_model['outlier_detection']
    pca_enabled = top_model['pca']
    pca_transform = top_model['pca_transform']

    final_result = {key: top_model[key] for key in model_keys_to_export if key in top_model}
    for key in ['optimizer', 'loss_function', 'activation_layers', 'outlier_dict', 'lr_schedule_strategy']:
        if key in final_result.keys():
            value = final_result[key]
            final_result[key] = replace_df_objects_with_functions(str(final_result[key]))
        
        
    # Save the entire model as a .h5 file (architecture, optimizer, and weights)
    model.save(folder_name + 'model_' + model_name + '.h5')

    # Save the scaler
#     with open(folder_name + 'scaler_' + model_name +  '.pkl', 'wb') as f:
#         pickle.dump(scaler_to_use, f)
        # Save the preprocessor to a file
#         joblib.dump(preprocessor, 'preprocessor.pkl')
        
    with open(folder_name + 'preprocessor_' + model_name + '.pkl', 'wb') as f:
        pickle.dump(preprocessor, f)

    # Save the list of features (e.g., column names)
    features = list(X.columns)

    with open(folder_name + 'features_' + model_name + '.json', 'w') as f:
        json.dump(features, f)


    # Save the dictionary
    with open(folder_name + 'modelresult_' + model_name +  '.pkl', 'wb') as f:
        pickle.dump(final_result, f)


    # Save the model to a file
    with open(folder_name + model_name +'_outlier_model.pkl', 'wb') as f:
        pickle.dump(outlier_model, f)


    # Save the PCA model to a file
    if pca_enabled:
        with open(folder_name + model_name + '_pca_transformer.pkl', 'wb') as f:
            pickle.dump(pca_transform, f)
            
            
    return
    

def replace_df_objects_with_functions(value_to_check):
    
    if pd.isna(value_to_check):  # Check for NaN
        return value_to_check
    
    # Replace activation functions
    if 'swish' in value_to_check:
        value_to_check = re.sub(r"<function swish at [^>]+>", 'tf.keras.activations.swish', value_to_check)
    if 'silu' in value_to_check:
        value_to_check = re.sub(r"<function silu at [^>]+>", 'tf.keras.activations.swish', value_to_check)
    if 'gelu' in value_to_check:
        value_to_check = re.sub(r"<function gelu at [^>]+>", 'tf.keras.activations.gelu', value_to_check)


    # Replace optimizers
    optimizer_replacements = {
        r"<class 'keras.src.optimizers.adam.Adam'>": 'tf.keras.optimizers.Adam',
        r"<class 'keras.optimizers.optimizer_experimental.adam.Adam'>": 'tf.keras.optimizers.Adam',
        r"<class 'keras.src.optimizers.rmsprop.RMSprop'>": 'tf.keras.optimizers.RMSprop',
        r"<class 'keras.src.optimizers.nadam.Nadam'>": 'tf.keras.optimizers.Nadam',
        r"<class 'keras.src.optimizers.adadelta.Adadelta'>": 'tf.keras.optimizers.Adadelta',
        r"<class 'keras.src.optimizers.adagrad.Adagrad'>": 'tf.keras.optimizers.Adagrad',
        r"<class 'keras.src.optimizers.adamax.Adamax'>": 'tf.keras.optimizers.Adamax',
        r"<class 'keras.optimizers.optimizer_experimental.adamax.Adamax'>": 'tf.keras.optimizers.Adamax',
        r"<class 'keras.src.optimizers.ftrl.Ftrl'>": 'tf.keras.optimizers.Ftrl',
        r"<class 'keras.optimizers.optimizer_experimental.sgd.SGD'>":'tf.keras.optimizers.SGD',
        r"<class 'keras.src.optimizers.sgd.SGD'>":'tf.keras.optimizers.SGD'
    }
    
    
    for pattern, replacement in optimizer_replacements.items():
        value_to_check = re.sub(pattern, replacement, value_to_check)

    # Replace loss functions
    loss_replacements = {
        r"<class 'keras.src.losses.MeanSquaredError'>": 'tf.keras.losses.MeanSquaredError',
        r"<class 'keras.src.losses.MeanAbsoluteError'>": 'tf.keras.losses.MeanAbsoluteError',
        r"<class 'keras.src.losses.losses.MeanAbsoluteError'>": 'tf.keras.losses.MeanAbsoluteError',
        r"<class 'keras.losses.MeanAbsoluteError'>": 'tf.keras.losses.MeanAbsoluteError',
        r"<class 'keras.losses.MeanSquaredError'>": 'tf.keras.losses.MeanAbsoluteError',
        r"<class 'keras.src.losses.MeanAbsolutePercentageError'>": 'tf.keras.losses.MeanAbsolutePercentageError',
        r"<class 'keras.src.losses.MeanSquaredLogarithmicError'>": 'tf.keras.losses.MeanSquaredLogarithmicError',
        r"<class 'keras.losses.MeanSquaredLogarithmicError'>": 'tf.keras.losses.MeanSquaredLogarithmicError',
        r"<class 'keras.src.losses.Huber'>": 'tf.keras.losses.Huber',
        r"<class 'keras.losses.Huber'>": 'tf.keras.losses.Huber',
        r"<class 'keras.src.losses.LogCosh'>": 'tf.keras.losses.LogCosh',
        r"<class 'keras.losses.LogCosh'>": 'tf.keras.losses.LogCosh',
        r"<class 'keras.src.losses.CategoricalCrossentropy'>": 'tf.keras.losses.CategoricalCrossentropy',
        r"<class 'keras.src.losses.BinaryCrossentropy'>": 'tf.keras.losses.BinaryCrossentropy',
        r"<class 'keras.losses.BinaryCrossentropy'>": 'tf.keras.losses.BinaryCrossentropy',
        r"<class 'keras.src.losses.losses.BinaryCrossentropy'>": 'tf.keras.losses.BinaryCrossentropy',
        r"<class 'keras.src.losses.KLDivergence'>": 'tf.keras.losses.KLDivergence',
        r"<class 'keras.src.losses.CosineSimilarity'>": 'tf.keras.losses.CosineSimilarity',
        r"<class 'keras.src.losses.Poisson'>": 'tf.keras.losses.Poisson',
        r"<class 'keras.losses.Poisson'>": 'tf.keras.losses.Poisson',
        r"<class 'keras.src.losses.losses.Poisson'>": 'tf.keras.losses.Poisson',
        r"<class 'keras.src.losses.Hinge'>": 'tf.keras.losses.Hinge',
        r"<class 'keras.src.losses.CategoricalHinge'>": 'tf.keras.losses.CategoricalHinge',
    }
    
    
    
    
    for pattern, replacement in loss_replacements.items():
        value_to_check = re.sub(pattern, replacement, value_to_check)
        
        
    if '<function exponential_decay_95' in value_to_check:
        value_to_check = re.sub(r"<function exponential_decay_95 at [^>]+>", 'exponential_decay_95', value_to_check)
    if '<function get_lr_schedule_strategies.<locals>.exponential_decay_95' in value_to_check:
        value_to_check = re.sub(r"<function get_lr_schedule_strategies.<locals>.exponential_decay_95 at [^>]+>", 'exponential_decay_95', value_to_check)
    if '<function exponential_decay_90' in value_to_check:
        value_to_check = re.sub(r"<function exponential_decay_90 at [^>]+>", 'exponential_decay_90', value_to_check)
    if '<function exponential_decay_85' in value_to_check:
        value_to_check = re.sub(r"<function exponential_decay_85 at [^>]+>", 'exponential_decay_85', value_to_check)
    if '<function exponential_decay_80' in value_to_check:
        value_to_check = re.sub(r"<function exponential_decay_80 at [^>]+>", 'exponential_decay_80', value_to_check)
        

    if '<function step_decay_50_10' in value_to_check:
        value_to_check = re.sub(r"<function step_decay_50_10 at [^>]+>", 'step_decay_50_10', value_to_check)
    if '<function step_decay_30_10' in value_to_check:
        value_to_check = re.sub(r"<function step_decay_30_10 at [^>]+>", 'step_decay_30_10', value_to_check)
    if '<function step_decay_50_5' in value_to_check:
        value_to_check = re.sub(r"<function step_decay_50_5 at [^>]+>", 'step_decay_50_5', value_to_check)
    if '<function step_decay_70_10' in value_to_check:
        value_to_check = re.sub(r"<function step_decay_70_10 at [^>]+>", 'step_decay_70_10', value_to_check)

    if '<function time_based_decay_1e4' in value_to_check:
        value_to_check = re.sub(r"<function time_based_decay_1e4 at [^>]+>", 'time_based_decay_1e4', value_to_check)
    if '<function time_based_decay_1e5' in value_to_check:
        value_to_check = re.sub(r"<function time_based_decay_1e5 at [^>]+>", 'time_based_decay_1e5', value_to_check)
    if '<function time_based_decay_5e4' in value_to_check:
        value_to_check = re.sub(r"<function time_based_decay_5e4 at [^>]+>", 'time_based_decay_5e4', value_to_check)
    if '<function time_based_decay_2e4' in value_to_check:
        value_to_check = re.sub(r"<function time_based_decay_2e4 at [^>]+>", 'time_based_decay_2e4', value_to_check)

    if '<function cosine_annealing_30' in value_to_check:
        value_to_check = re.sub(r"<function cosine_annealing_30 at [^>]+>", 'cosine_annealing_30', value_to_check)
    if '<function cosine_annealing_50' in value_to_check:
        value_to_check = re.sub(r"<function cosine_annealing_50 at [^>]+>", 'cosine_annealing_50', value_to_check)
    if '<function cosine_annealing_10' in value_to_check:
        value_to_check = re.sub(r"<function cosine_annealing_10 at [^>]+>", 'cosine_annealing_10', value_to_check)
    if '<function cosine_annealing_20' in value_to_check:
        value_to_check = re.sub(r"<function cosine_annealing_20 at [^>]+>", 'cosine_annealing_20', value_to_check)

    if '<function reduce_lr_50_5' in value_to_check:
        value_to_check = re.sub(r"<function reduce_lr_50_5 at [^>]+>", 'reduce_lr_50_5', value_to_check)
    if '<function reduce_lr_30_5' in value_to_check:
        value_to_check = re.sub(r"<function reduce_lr_30_5 at [^>]+>", 'reduce_lr_30_5', value_to_check)
    if '<function reduce_lr_50_10' in value_to_check:
        value_to_check = re.sub(r"<function reduce_lr_50_10 at [^>]+>", 'reduce_lr_50_10', value_to_check)
    if '<function reduce_lr_70_5' in value_to_check:
        value_to_check = re.sub(r"<function reduce_lr_70_5 at [^>]+>", 'reduce_lr_70_5', value_to_check)
        
        
    if 'Reduce on Plateau 50%, patience 5' in value_to_check:
        value_to_check = re.sub(r"{'method': <keras.src.callbacks.ReduceLROnPlateau [^}]+}", "{'method': reduce_lr_50_5, 'description': 'Reduce on Plateau 50%, patience 5'}", value_to_check)
        value_to_check = re.sub(r"{'method': <keras.callbacks.ReduceLROnPlateau [^}]+}", "{'method': reduce_lr_50_5, 'description': 'Reduce on Plateau 50%, patience 5'}", value_to_check)
    if 'Reduce on Plateau 30%, patience 5' in value_to_check:
        value_to_check = re.sub(r"{'method': <keras.src.callbacks.ReduceLROnPlateau [^}]+}", "{'method': reduce_lr_30_5, 'description': 'Reduce on Plateau 30%, patience 5'}", value_to_check)
        value_to_check = re.sub(r"{'method': <keras.callbacks.ReduceLROnPlateau [^}]+}", "{'method': reduce_lr_30_5, 'description': 'Reduce on Plateau 30%, patience 5'}", value_to_check)
    if 'Reduce on Plateau 50%, patience 10' in value_to_check:
        value_to_check = re.sub(r"{'method': <keras.src.callbacks.ReduceLROnPlateau [^}]+}", "{'method': reduce_lr_50_10, 'description': 'Reduce on Plateau 50%, patience 10'}", value_to_check)
        value_to_check = re.sub(r"{'method': <keras.callbacks.ReduceLROnPlateau [^}]+}", "{'method': reduce_lr_50_10, 'description': 'Reduce on Plateau 50%, patience 10'}", value_to_check)
    if 'Reduce on Plateau 70%, patience 5' in value_to_check:
        value_to_check = re.sub(r"{'method': <keras.src.callbacks.ReduceLROnPlateau [^}]+}", "{'method': reduce_lr_70_5, 'description': 'Reduce on Plateau 70%, patience 5'}", value_to_check)
        value_to_check = re.sub(r"{'method': <keras.callbacks.ReduceLROnPlateau [^}]+}", "{'method': reduce_lr_70_5, 'description': 'Reduce on Plateau 70%, patience 5'}", value_to_check)
        
        
        
        
    return value_to_check




def remove_high_null_features(df, features_to_check, threshold):
    
    for col in features_to_check:
        null_counts = (df[col].isnull().sum() / len(df))
        if null_counts > 0:
            print('Column  %s with nulls %s'%(col, round(null_counts*100,1)))

            if null_counts > threshold:
                print('Dropping column')
                print('')
                df.drop(col, axis = 1, inplace = True)
                features_to_check.remove(col)
            
    return df, features_to_check


def impute_missing_values(df, features_to_impute):
    
    for feature in features_to_impute:
        
        if feature in df.columns:

            if df[feature].isnull().sum() > 0:

                feature_type = df[feature].dtype

                if feature_type == 'object':
                    print('Replacing %s with mode: %s'%(feature, df[feature].mode()[0]))
                    df[feature] = df[feature].fillna(df[feature].mode()[0])
                else:
                    print('Replacing %s with median: %s'%(feature, df[feature].median()))
                    df[feature] = df[feature].fillna(df[feature].median())
        else:
            print('Cant impute as this feature has already been removed: %s'%(feature))
            
    return df





def remove_highly_correlated_features(df, features_to_use=None, threshold=0.95, priority_features=None):
    """
    Removes highly correlated features from a DataFrame, prioritizing the retention 
    of specified features.

    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    - features_to_use (list): List of features to check (if None, use all numeric columns).
    - threshold (float): Correlation threshold above which a feature is removed.
    - priority_features (list): List of features to retain even if highly correlated.

    Returns:
    - df (pd.DataFrame): The cleaned DataFrame with highly correlated features removed.
    """
    
    if features_to_use is None:
        features_to_use = df.select_dtypes(include=[np.number]).columns.tolist()

    if priority_features is None:
        priority_features = []  # Default to empty list if not provided

    # Compute correlation matrix
    corr_matrix = df[features_to_use].corr().abs()

    # Get upper triangle (excluding diagonal)
    upper_tri = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))

    # Find highly correlated features to drop
    to_drop = set()
    for column in upper_tri.columns:
        for index in upper_tri.index:
            if index == column:
                continue  # Skip self-comparisons

            value = upper_tri.at[index, column]

            if pd.notna(value) and isinstance(value, (int, float)) and (value > threshold):
                # Check if either feature is in the priority list
                if column in priority_features and index not in priority_features:
                    # Retain the priority feature and drop the correlated one
                    to_drop.add(index)
                    print(f"📌 Removing '{index}' (correlated with '{column}', r={value:.2f})")
                elif index in priority_features and column not in priority_features:
                    # Retain the priority feature and drop the correlated one
                    to_drop.add(column)
                    print(f"📌 Removing '{column}' (correlated with '{index}', r={value:.2f})")
                else:
                    # If neither feature is prioritized, drop the column (default behavior)
                    to_drop.add(column)
                    print(f"📌 Removing '{column}' (correlated with '{index}', r={value:.2f})")
                break  # Remove only one feature per pair

    # Drop selected features
    df_cleaned = df.drop(columns=list(to_drop), errors="ignore")

    return df_cleaned




def compute_vif(data, features):
    """ Computes VIF for all features in parallel. """
    X = data[features].values
    with Pool() as pool:
        vif_values = pool.starmap(variance_inflation_factor, [(X, i) for i in range(X.shape[1])])
    return pd.Series(vif_values, index=features)

def remove_high_vif_features(df, features_to_use, threshold=10):
    """
    Removes features with high Variance Inflation Factor (VIF) to reduce multicollinearity.
    Optimized using multiprocessing.
    
    Parameters:
    - df (pd.DataFrame): DataFrame containing numerical features.
    - features_to_use (list): List of feature names to consider.
    - threshold (float): VIF threshold for removing features (default = 10).
    
    Returns:
    - df (pd.DataFrame): The cleaned DataFrame.
    - dropped_features (list): List of features that were removed.
    """
    dropped_features = []
    
    while True:
        vif_data = compute_vif(df, features_to_use)
        
        max_vif = vif_data.max()
        if max_vif < threshold:
            break  # Stop if no features exceed the threshold

        # Drop the feature with the highest VIF
        feature_to_drop = vif_data.idxmax()
        features_to_use.remove(feature_to_drop)
        df = df.drop(columns=[feature_to_drop])
        dropped_features.append(feature_to_drop)

        print(f"📌 Removing '{feature_to_drop}' due to high VIF ({max_vif:.2f})")

    return df, dropped_features



def drop_categorical_vars_with_too_many_categories(df, cat_cols, threshold_perc):

    unique_perc = (df[cat_cols].nunique() / df[cat_cols].count()) * 100

    cols_to_drop = unique_perc[unique_perc > threshold_perc].index.tolist()
    df.drop(cols_to_drop, axis=1, inplace=True)

    cat_cols = df.select_dtypes(include=['object']).columns.tolist()
    unique_perc = (df[cat_cols].nunique() / df[cat_cols].count()) * 100
    cols_to_drop = unique_perc[unique_perc == 100].index.tolist()
    df.drop(cols_to_drop, axis=1, inplace=True)

    return df


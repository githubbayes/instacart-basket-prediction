import os

import pandas as pd


def parse_order(x):
    series = pd.Series()
    '''将一个order里面的所有信息序列化'''
    series['products'] = '_'.join(x['product_id'].values.astype(str).tolist())
    series['reorders'] = '_'.join(x['reordered'].values.astype(str).tolist())
    # 1 if this product has been ordered by this user in the past, 0 otherwise in the level of order products
    series['aisles'] = '_'.join(x['aisle_id'].values.astype(str).tolist())
    series['departments'] = '_'.join(x['department_id'].values.astype(str).tolist())

    series['order_number'] = x['order_number'].iloc[0]
    # the order sequence number for this user
    series['order_dow'] = x['order_dow'].iloc[0]
    # the day of the week the order was placed on
    series['order_hour'] = x['order_hour_of_day'].iloc[0]
    # the hour of the day the order was placed on
    series['days_since_prior_order'] = x['days_since_prior_order'].iloc[0]
    #days since the last order, capped at 30 (with NAs for order_number = 1)
    return series


def parse_user(x):
    parsed_orders = x.groupby('order_id', sort=False).apply(parse_order)

    series = pd.Series()

    series['order_ids'] = ' '.join(parsed_orders.index.map(str).tolist())
    # one user bookings order id list
    series['order_numbers'] = ' '.join(parsed_orders['order_number'].map(str).tolist())
    # one user order sequence
    series['order_dows'] = ' '.join(parsed_orders['order_dow'].map(str).tolist())
    # the day of the week the order was placed on
    series['order_hours'] = ' '.join(parsed_orders['order_hour'].map(str).tolist())
    series['days_since_prior_orders'] = ' '.join(parsed_orders['days_since_prior_order'].map(str).tolist())

    series['product_ids'] = ' '.join(parsed_orders['products'].values.astype(str).tolist())
    series['aisle_ids'] = ' '.join(parsed_orders['aisles'].values.astype(str).tolist())
    series['department_ids'] = ' '.join(parsed_orders['departments'].values.astype(str).tolist())
    series['reorders'] = ' '.join(parsed_orders['reorders'].values.astype(str).tolist())

    series['eval_set'] = x['eval_set'].values[-1]

    return series

if __name__ == '__main__':
    orders = pd.read_csv('~/Downloads/orders.csv',nrows = 100000)
    prior_products = pd.read_csv('~/Downloads/order_products_prior.csv',nrows = 100000)
    train_products = pd.read_csv('~/Downloads/order_products_train.csv',nrows = 100000)
    order_products = pd.concat([prior_products, train_products], axis=0)
    products = pd.read_csv('~/Downloads/products.csv')

    df = orders.merge(order_products, how='left', on='order_id')
    df = df.merge(products, how='left', on='product_id')
    df['days_since_prior_order'] = df['days_since_prior_order'].fillna(0).astype(int)
    null_cols = ['product_id', 'aisle_id', 'department_id', 'add_to_cart_order', 'reordered']
    df[null_cols] = df[null_cols].fillna(0).astype(int)

    if not os.path.isdir('~/Downloads'):
        os.makedirs('~/Downloads')

    user_data = df.groupby('user_id', sort=False).apply(parse_user).reset_index()
    user_data.to_csv('~/Downloads/user_data.csv', index=False)

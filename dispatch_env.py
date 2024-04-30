import pandas as pd
import json
import datetime as dt


def load_entire_order_info():
    order_file = "all_waybill_info_meituan.csv"
    order_df = pd.read_csv(order_file)
    order_df[['dt']] = order_df[['dt']].astype(str) 
    order_df['dt'] = pd.to_datetime(order_df['dt'])
    return order_df


def load_dispatch_order_info():
    # 每一个时间段 需要被分单的订单数据 15921-lines
    dispatch_order_file = "dispatch_waybill_meituan.csv"
    dispatch_order_df = pd.read_csv(dispatch_order_file)
    dispatch_order_df[['dt']] = dispatch_order_df[['dt']].astype(str) 
    dispatch_order_df['dt'] = pd.to_datetime(dispatch_order_df['dt'])
    return dispatch_order_df


def load_dispatch_rider_info():
    # 每一个时间段 可以使用的骑手及背单信息 62044-lines
    dispatch_rider_file = "dispatch_rider_meituan.csv"
    dispatch_rider_df = pd.read_csv(dispatch_rider_file)
    dispatch_rider_df[['dt']] = dispatch_rider_df[['dt']].astype(str) 
    dispatch_rider_df['dt'] = pd.to_datetime(dispatch_rider_df['dt'])
    return dispatch_rider_df


def main():
    entire_order_df = load_entire_order_info()
    entire_order_group = entire_order_df.groupby(by=['dt'])

    groups = ['dt', 'dispatch_time']
    dispatch_order_df = load_dispatch_order_info()
    dispatch_order_group = dispatch_order_df.groupby(by=groups)
    
    dispatch_rider_df = load_dispatch_rider_info()
    dispatch_rider_group = dispatch_rider_df.groupby(by=groups)

    for k, v in dispatch_order_group:
        rider_info = dispatch_rider_group.get_group(k)
        rider_info_dict = {}
        order_in_rider = set()
        for _,row in rider_info.iterrows():
            rider_id = row['courier_id']
            if rider_info_dict.__contains__(rider_id):
                raise KeyError("Duplicated rider-id {}: {}".format(rider_id, dt.datetime.fromtimestamp(k[-1])))
            order_list = json.loads(row["courier_waybills"])
            rider_info_dict[row['courier_id']] = {
                'id': row['courier_id'],
                'rider_lat': row['rider_lat'],
                'rider_lng': row['rider_lng'],
                'courier_waybills': order_list
                }
            order_in_rider.update(order_list)
        order_to_be_dipatched = list(v['order_id'].values)
        # 需要dispatch的order + 骑手背单 详细信息
        order_pool = entire_order_group.get_group(k[0])
        order_keys = set()
        order_keys.update(order_in_rider)
        order_keys.update(order_to_be_dipatched)

        order_details = order_pool.loc[order_pool['order_id'].isin(order_keys)]
        print(order_details)
        break


if __name__ == "__main__":
    main()
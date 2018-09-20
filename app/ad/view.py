import asyncio
import time
import datetime

from sanic import Blueprint
from sanic.response import json
from aiomysql import create_pool


loop = asyncio.get_event_loop()

ad_blueprint = Blueprint("ad_blueprint", url_prefix="/ad")

# ad_blueprint.config.update(dict(MYSQL=dict(host="172.16.1.251", port=3306, user='root', password='', db='')))

@ad_blueprint.listener('before_server_start')
async def start_mysql(app, loop):
    ad_mysql_config = app.config.get("MYSQL")
    if not ad_mysql_config:
        raise RuntimeError("ad mysql config not empty.")

    _mysql = await create_pool(**ad_mysql_config)
    async def _query(sqlstr, args=None):
        async with _mysql.acquire() as conn:
            async with conn.cursor() as cur:
                final_str = cur.mogrify(sqlstr, args)
                print('mysql query [{}]'.format(final_str))
                await cur.execute(final_str)
                value = await cur.fetchall()
                return value

    setattr(_mysql, 'query', _query)

    app.mysql = _mysql

@ad_blueprint.listener('after_server_stop')
async def close_mysql(app, loop):
    app.mysql.close()
    await app.mysql.wait_closed()


@ad_blueprint.route("/foo")
async def foo(request):
    msg = await request.app.mysql.query("SELECT * FROM `zg_ads` where deleted_at is null;")

    return json({"msg": msg})

def get_today():
    return datetime.date.today()

def get_yesterday():
    return datetime.timedelta(days=1)

def get_timestamp(datestr):
    return int(time.mktime(time.strptime(str(datestr), "%y-%m-%d")))

def time_query_sql(start_time, end_time, sql, query_time="created_at"):
    sql_str = ''
    msg = ''
    if start_time and end_time:
        try:
            start_time = int(start_time)
            end_time = int(end_time)
        except:
            msg = 'timestamp must int'
        else:
            if start_time < end_time:
                sql_str = '{} where {}>{} and {}<{}'.format(sql, query_time, start_time, query_time, end_time)
            else:
                msg = 'start_time must litter end_time'
    else:
        msg = '参数不完整'

    return sql_str, msg

@ad_blueprint.route("/adverstiser")
async def foo(request):
    ## yesterday_end_time = get_timestamp(get_today()) - 1
    ##yesterday_start_time = get_timestamp(get_yesterday())
    desc = '1只登录一次 2登录了不只一次'
    data = await request.app.mysql.query("select t2.t1, count(1) from (select case when updated_at=created_at then 1 else 2 end as t1 from zg_advertisers) t2 group by t2.t1")
    return json({"data": data, "code": 0, "desc": desc})

@ad_blueprint.route("/time/adverstiser")
async def time_adverst(request):
    '''当天的时间分布数据
    '''
    # today_start_time = get_timestamp(get_today())
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")
    time_type = request.args.get("types")

    code = -1
    if time_type == "day":
        time_strf = "%y-%m-%d %H"
    else:
        time_strf = "%y-%m-%d"

    sql = 'SELECT FROM_UNIXTIME(updated_at, "{}") as timestr, count(1)  FROM `zg_advertisers`'.format(time_strf)
    sql_str, msg = time_query_sql(start_time, end_time, sql, query_time="updated_at")

    data = []
    if sql_str:
        code = 0
        sql_str = '%s GROUP BY timestr;'%sql_str
        data = await request.app.mysql.query(sql_str)
    return json({"data": data, "code": code, "msg": msg})

@ad_blueprint.route("/time/orders")
async def time_order(request):
    '''一段时间内的各订单数据统计
    '''
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")
    time_type = request.args.get("types")   # 如果types为day，则按%y-%m-%d %H分组的，如果不是则按%y-%m-%d分组

    code = -1
    if time_type == "day":
        time_strf = "%y-%m-%d %H"
    else:
        time_strf = "%y-%m-%d"

    sql = 'SELECT FROM_UNIXTIME(created_at, "{}") as timestr, order_status, count(1) FROM `zg_orders`'.format(time_strf)
    sql_str, msg = time_query_sql(start_time, end_time, sql)

    data = []
    if sql_str:
        code = 0
        sql_str = '%s GROUP BY timestr, order_status ORDER BY timestr' % sql_str
        data = await request.app.mysql.query(sql_str)

    return json({"data": data, "code": code, "msg": msg})

@ad_blueprint.route("/time/orders_user")
async def orders_user(request):
    '''一段时间内订单数据 -- 按用户、订单状态分类
    '''
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    code = -1
    msg = ''
    data = []

    try:
        start_time = int(start_time)
        end_time = int(end_time)
    except:
        msg = '时间格式必须为int型'
    else:
        code = 0
        sql = 'SELECT advertiser_id, order_status, count(1) FROM `zg_orders` WHERE \
                created_at > %d and created_at < %d GROUP BY advertiser_id, order_status' % (start_time, end_time)

        data = await request.app.mysql.query(sql)
    return json({"data": data, "code": code, "msg": msg})


# 优惠券分析
# 优惠券是按创建时间来查询的

@ad_blueprint.route("/time/coupon")
async def time_coupon(request):
    '''优惠券发放统计
    '''
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    code = -1
    desc = '0 后台发放用  1 系统发放用'

    sql = 'SELECT FROM_UNIXTIME(created_at, "%y-%m-%d") as timestr, coupon_type, count(1) FROM `zg_coupons` '
    sql_str, msg = time_query_sql(start_time, end_time, sql)

    data = []
    if sql_str:
        code = 0
        sql_str = '%s GROUP BY timestr, coupon_type;' % sql_str
        data = await request.app.mysql.query(sql_str)

    return json({"data": data, "code": code, "msg": msg, "desc": desc})

@ad_blueprint.route("/time/coupons_status")
async def time_coupons_status(request):
    '''优惠券是否使用情况
    '''
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    code =  -1
    desc = '1未使用 2已使用'

    sql = 'SELECT coupon_status, count(1) FROM `zg_advertisers_coupons` '
    sql_str, msg = time_query_sql(start_time, end_time, sql)

    data = []
    if sql_str:
        code = 0
        sql_str = '%s GROUP BY coupon_status' % sql_str
        data = await request.app.mysql.query(sql_str)

    return json({"data": data, "code": code, "msg": msg, "desc": desc})

@ad_blueprint.route("/time/coupon_id_status")
async def time_coupons_id_status(request):
    '''优惠券是否使用情况 -- 每个优惠券
    '''
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    code = -1
    desc = '1未使用 2已使用'

    sql = 'SELECT coupon_id, coupon_status, count(1) FROM `zg_advertisers_coupons`'
    sql_str, msg = time_query_sql(start_time, end_time, sql)

    data = []
    if sql_str:
        code = 0
        sql_str = '%s GROUP BY coupon_id, coupon_status' % sql_str
        data = await request.app.mysql.query(sql_str)

    return json({"data": data, "code": code, "msg": msg, "desc": desc})

@ad_blueprint.route("/time/coupon_adver_status")
async def time_coupon_adver_status(request):
    '''优惠券是否使用情况 -- 每个广告主
    '''
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    code = -1
    desc = '1 未使用 2 已使用'

    sql = 'SELECT advertiser_id, coupon_status, count(1) FROM `zg_advertisers_coupons` '
    sql_str, msg = time_query_sql(start_time, end_time, sql)

    data = []
    if sql_str:
        code = 0
        sql_str = '%s GROUP BY advertiser_id, coupon_status ' % sql_str
        data = await request.app.mysql.query(sql_str)

    return json({"data": data, "code": code, "msg": msg, "desc": desc})

@ad_blueprint.route("/time/coupons_msg")
async def time_coupon_msg(request):
    '''优惠券消息是否已读
    '''
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    code = -1
    desc = '0 未读 1 已读'

    sql = 'SELECT is_read, count(1) FROM `zg_advertisers_coupons` '
    sql_str, msg = time_query_sql(start_time, end_time, sql)

    data = []
    if sql_str:
        code = 0
        sql_str = '%s GROUP BY is_read' % sql_str
        data = await request.app.mysql.query(sql_str)

    return json({"data": data, "code": code, "msg": msg, "desc": desc})

@ad_blueprint.route("/time/coupon_id_msg")
async def coupon_id_msg(request):
    '''优惠券消息是否已读 -- 每个优惠券
    '''

    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    code = -1
    desc = '0 未读 1 已读'

    sql = 'SELECT coupon_id, is_read, count(1) FROM `zg_advertisers_coupons` '
    sql_str, msg = time_query_sql(start_time, end_time, sql)

    data = []
    if sql_str:
        code = 0
        sql_str = '%s GROUP BY coupon_id, is_read' %sql_str
        data = await request.app.mysql.query(sql_str)

    return json({"data": data, "code": code, "msg": msg, "desc": desc})

@ad_blueprint.route("/time/coupon_adver_msg")
async def coupon_adver_msg(request):
    '''优惠券是否已读 -- 广告主
    '''
    start_time = request.args.get("start_time")
    end_time = request.args.get("end_time")

    code = -1
    desc = '0 未读 1 已读'

    sql = 'SELECT advertiser_id, is_read, count(1) FROM `zg_advertisers_coupons` '
    sql_str, msg = time_query_sql(start_time, end_time, sql)

    data = []
    if sql_str:
        code = 0
        sql_str = '%s GROUP BY advertiser_id, is_read' % sql_str
        data = await request.app.mysql.query(sql_str)

    return json({"data": data, "code": code, "msg": msg, "desc": desc})

@ad_blueprint.route("/time/coupon_expired_status")
async def coupon_expired_status(request):
    '''优惠券到期时间 -- 使用状态, 是否已读状态
    '''
    expired = request.args.get("expired")

    code = -1
    msg = ''
    data = []
    desc = '下标1: 1未使用 2已使用, 下标2: 0未读 1已读'

    try:
        expired = int(expired)
    except:
        msg = "过期时间必须为int类型" 
    else:
        now_time = time.time()
        if now_time < expired:
            sql_str = 'SELECT coupon_status, is_read, count(1) FROM `zg_advertisers_coupons` WHERE \
                    coupon_expired > %d and coupon_expired < %d GROUP BY coupon_status, is_read' %(
                        time.time(), expired)
            data = await request.app.mysql.query(sql_str)
        else:
            msg = '过期时间必须大于当前时间'

    return json({"data": data, "code": code, "msg": msg, "desc": desc})


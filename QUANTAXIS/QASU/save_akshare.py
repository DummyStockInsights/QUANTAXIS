#
# The MIT License (MIT)
#
# Copyright (c) 2016-2021 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from datetime import date

import akshare as ak
import pymongo

from QUANTAXIS.QAFetch.QATdx import QA_fetch_get_stock_list
from QUANTAXIS.QAUtil import QA_util_to_json_from_pandas, QA_util_log_info
from QUANTAXIS.QAUtil.QASetting import DATABASE


def QA_SU_save_stocks_news_day_top100(client=DATABASE):
    """save all stocks' news for current dat via AKShare from 东方财富.
    Help page URL is http://so.eastmoney.com/news/s.

    """
    stock_list = QA_fetch_get_stock_list().code.unique().tolist()

    for index, code in enumerate(stock_list):
        summary_msg = "The {} of Total {}".format(index, len(stock_list))
        progress_msg = "DOWNLOAD PROGRESS {} ".format(
            str(index / len(stock_list) * 100)[0:4] + "%"
        )

        QA_util_log_info(summary_msg + ". " + progress_msg)

        QA_SU_save_stock_news_day_top100(code, client=client)

    print(stock_list)


def QA_SU_save_stock_news_day_top100(
    code: str, engine="akshare", client=DATABASE
):
    """save a stock's news for current day via AKShare from 东方财富.
    Help page URL is http://so.eastmoney.com/news/s.

    """
    today = date.today()

    QA_util_log_info(
        "## Saving news for stock [{}] ({})...".format(code, today)
    )

    try:
        stock_news_em_df = ak.stock_news_em(symbol=code)
    except Exception as e:
        print(e)
        print("Will skip")
        return

    stock_news_em_df.rename(
        columns={
            "关键词": "code",
            "发布时间": "publish_time",
            "文章来源": "publisher",
            "新闻链接": "news_link",
            "新闻标题": "title",
            "新闻内容": "content",
        },
        inplace=True,
    )

    json_data = QA_util_to_json_from_pandas(stock_news_em_df)

    coll = client.stock_news_akshare_dfcf
    coll.create_index(
        [
            ("code", pymongo.ASCENDING),
            ("publish_time", pymongo.ASCENDING),
            ("news_link", pymongo.ASCENDING),
        ],
        unique=True,
    )

    try:
        coll.insert_many(json_data)
    except pymongo.errors.BulkWriteError as bwe:
        noted_err = [
            detail
            for detail in bwe.details["writeErrors"]
            if "duplicate key error" in detail["errmsg"]
        ]

        if not noted_err:
            raise

    QA_util_log_info(
        "## {} News for stock [{}] saving done ({}).".format(
            len(stock_news_em_df), code, today
        )
    )

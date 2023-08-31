# Financial_Data_Crawler

+ **文件说明**

  此仓库主要包含如下代码文件：

  + Investing爬虫

    + 总表爬虫汇总：爬取Investing的总表(不包含国债收益率的表！
    + 事件爬虫汇总：是总表的子表，只需要爬取我们目前所做的事件的表

  + 金十爬虫：和Investing几乎独立，需要保证事件发生的时候，及时更新！

    

+ **可能错误及解决办法**

  在上传到代码仓库之前，均在本地做了相关的测试，程序在上传仓库之前已无问题，当出现代码问题的时候请根据解释器的提示进行修改！**常见的问题如下：**

  + **解释器报出路径错误！**

    + 请根据本人的电脑选择好对应的路径，特别是当本地电脑不存在相关文件夹的时候，请创建相关的文件夹！

  + **金十爬虫程序中的`jin10_in_time.py`程序一直运行陷入死循环**

    + 请注意检查即将到来的事件及jinshi_id是否完全在今天公布的财经日历中，当不满足上述条件的时候，程序会认为并没有公布所有事件，会一直循环爬虫知道拿到即将更新的事件数据为止！

  + **更新Investing总表事件的时候没能更新最新值**

    + Investing网站打开速度较慢，可以适当牺牲时间换取准确率，可以设置多线程的进程数少一点，同时给浏览器更多的缓冲时间。当明确知道哪个表或哪个事件没有更新今值的时候，可以采用`指定表名爬虫.py`和`更新指定事件.py`文件进行修补

  + **某些csv文件会显示编码错误**

    + 可能用`utf-8`的编码方式会出错，因此如果出现错误需要广泛选择csv文件的编码方式

  + **出现`Chrome drive`和`Google浏览器`不匹配的错误或浏览器闪退的错误**

    + 请检查Google版本是否发生了自动更新，并去[以下网站](https://googlechromelabs.github.io/chrome-for-testing/)下载老版本的Google以及Chrome driver。指定Google以及Chrome driver的参考代码如下：

      ```python
      from selenium import webdriver
      from selenium.webdriver.chrome.options import Options
      
      options = Options()
      options.binary_location = "C:\\chrome_x32\\Chrome-bin\\chrome.exe"
      driver = webdriver.Chrome(chrome_options=options,executable_path="C:/chrome_x32/chromedriver_win32/chromedriver")
      driver.get('http://www.baidu.com')
      ```

      

+ **建议**

  主要建议如下：

  + 在上传到数据库之前，最好检查以下本地文件的csv文件是否存在且无错误，更新完成后也记得去SQL Sever中检查一遍

    

+ **作者**

  [陈颖航](https://github.com/jason51108)


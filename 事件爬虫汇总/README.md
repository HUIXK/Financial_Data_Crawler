# EP程序说明

- **项目背景**

  根据`investing彭博字段表_最终版(新版).xlsx`爬取`Investing`网站对应的**事件数据**，并存储到数据库

  

- **程序设计流程：**

  1. 去`Invseting`网站爬取对应的**事件数据(例如：EIA、UNE等)**，将爬取到的数据保存在`DATA\TXT\{事件名}`文件夹下
  2. 读取`DATA\TXT\{事件名}`文件夹下下的`.txt`文件，进行一定的格式转化**(利用正则表达式进行转化)**，和阿里云数据库中的表进行拼接、去重，拼接好的新表会存放在本地的`DATA\CSV\{事件名}`文件夹下。
  3. 清空阿里云数据库的表，并将`DATA\CSV\{事件名}`文件夹下的所有`.csv`文件存入数据库



- **文件说明：**

  - `data`：放置爬取下来的`.txt`文件以及处理好后的`.csv`文件

  + `feature_table`：主程序，包含文件如下：
    + `feature(未用).xlsx`：翟给的表，我暂时未用

    + `investing彭博字段表_最终版(新版).xlsx`表：根据这个表去爬取数据
    + `update.py`：更新所有的事件表，与总表程序里面的`事件总表.py`程序类似
    + `update_in_time(fast).py`：定时抓取即将到来事件的实际值，可实现迅速抓取
    + `update_in_time(暂时未用).py`：老版本的代码，抓取即将到来的实际值，抓取速度慢
    + `feature.ipynb`：测试文件，当程序出现问题时可在`jupyter`进行测试
  + `trading_calendar`：2023年期间，财经日历对应事件的事件
    + `trading_calendar.csv`：2023年期间，财经日历对应事件的事件
    + `trading_calendar.xlsx`：2023年期间，财经日历对应事件的事件

> 注意：
>
> 1、`update_in_time(fast).py`的运行逻辑是先打开浏览器，等到事件到了进行获取数据；而`update_in_time(暂时未用).py`是时间到了再打开浏览器进行爬虫
>
> 2、`update.py`与总表程序里面的`事件总表.py`程序类似，多了一个合并合并feature字段的代码



- **安装方法**

  在运行程序之前，请确保安装了以下必要的第三方库

  - Selenium以及对应Google浏览器版本的[Chrome driver](https://googlechromelabs.github.io/chrome-for-testing/)
  - Pandas
  - Numpy

  对于没有相关的依赖库，需要使用`pip install '库名'`或者`conda install '库名'`，**选取解释器时，注意Conda环境下是否存在相关的依赖库，建议使用Conda下的base环境。**

  

- **上手指南**

  + `update.py`程序

    `update.py`程序可以参考总表爬虫的相关说明，现展示此代码构建featrue的一部分

    逻辑：读取每个事件下的所有`.csv`文件，文件按照事件进行内连接，核心代码`df = pd.merge(df, df_list[k], on='datetime', how='inner')`；连接后存放入新的DataFrame中，用逗号分隔，核心代码`df_new['feature'] = df.iloc[:, 1:].apply(lambda row: ','.join(row.astype(str)), axis=1)`，最终输出到`data/CSV/EVENT`文件夹下

    ```
    # 读取每个一个事件的名字
    for event in list_model_name:
        try:
            path_ = rf'../data/CSV/'
            path = os.path.join(path_, event)
            path_list = [os.path.join(path, i) for i in os.listdir(path)]
            df_list = []
            for path__ in path_list:
                df = pd.read_csv(path__)
                df['datetime'] = pd.to_datetime(df['datetime'])
                df['datetime'] = df['datetime'].dt.date
                try:
                    df.drop('Is_P', axis=1, inplace=True)
                except:
                    pass
                df_list.append(df)
            df = df_list[0]
            for k in range(1, len(df_list)):
                df = pd.merge(df, df_list[k], on='datetime', how='inner')
            df.replace('None', 0, inplace=True)
            df.fillna(0, inplace=True)
            df_new = pd.DataFrame({'date': df['datetime'], 'feature': None})
            df_new.iloc[:, 0] = pd.to_datetime(df_new.iloc[:, 0])  # 转化为时间格式
            df_new['feature'] = df.iloc[:, 1:].apply(lambda row: ','.join(row.astype(str)), axis=1)
            try:
                df_2 = read_db_df(event)
                df_2.iloc[:, 0] = pd.to_datetime(df_2.iloc[:, 0])  # 转化为时间格式
                df_2 = pd.concat([df_new, df_2])
                df_2 = df_2.drop_duplicates(subset='datetime')
                df_2.to_csv(rf'../data/CSV/EVENT/{event}.csv', index=False)
            except:
                df_new.to_csv(rf'../data/CSV/EVENT/{event}.csv', index=False)
        except:
            print(f'{event}的feature有问题，请注意检查')
            pass
    ```

  + `update_in_time(fast).py`程序

    代码比较冗长(原因在于将)，后续可以做继续修改。核心代码

    ```python
    # 如果离事件到达的事件大于8分钟(预留足够的事件打开浏览器)，则每次打印后休息一分钟
    if (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 8*60:
    	print(f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}")
        time.sleep(60)
    
    # 如果离事件到达的事件小于8分钟，则开始打开浏览器，并在浏览器完全打开后每两秒输出
    elif (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() < 8*60:
    	print('已打开浏览器随时待命！！')
        df = pd.read_excel(r'investing彭博字段表_最终版.xlsx', sheet_name='EP_EVENT(最新)')
        df['Model_Name'].fillna(method='ffill', inplace=True)
        base_url = 'https://cn.investing.com/economic-calendar/'
        list_url = [base_url + i for i in df[df['Model_Name'] == upcoming_event]['Investing_Id']]
        list_table_name = df[df['Model_Name'] == upcoming_event]['Table_Name'].tolist()
        options = webdriver.ChromeOptions()  # 谷歌浏览器
        options.page_load_strategy = 'eager'
        options.add_experimental_option("detach", True)
        options.add_argument("blink-settings=imagesEnabled=false")  # 不加载图片
        options.add_argument("--disable-extensions")  # 禁用插件加载
        options.add_argument("--disable-gpu")  # 禁止gpu
        # options.add_argument("--disable-software-rasterizer")  # 无界面
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--start-maximized")  # 窗口全屏显示
    
    	drivers = [webdriver.Chrome(options=options) for _ in range(len(list_url))]  # 创建浏览器实例列表
        for driver, url in zip(drivers, list_url):  # 同时打开所有的浏览器
        	driver.get(url)
        while (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 0:
        	print(f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}\t >>>>>>>>>>> \t剩余时间{(pd.to_datetime(upcoming_time) - datetime.now()).total_seconds()}")
            time.sleep(2)
    # 其实就二分类，大于8分钟和小于8分钟，因此，这里的else基本不会执行(前提是if和elif是对立事件！)
    else:
    	print(f'你已错过{upcoming_event}事件！')
    ```

    主程序

    ```python
    # 只要今年没结束，程序就会一直运行
    while (pd.to_datetime('2023-12-31') - datetime.now()).total_seconds() > 0:
        event_path = r"../trading_calendar/trading_calendar.csv"
        event_df = pd.read_csv(event_path)
        event_df['datetime'] = pd.to_datetime(event_df['datetime'])
        upcoming_time = event_df[event_df['datetime'] > datetime.now()].min()['datetime']
        upcoming_event = event_df[event_df['datetime'] == upcoming_time]['model'].values[0]
        #upcoming_time +=  pd.Timedelta(seconds=10)
        #upcoming_event = 'UNE'
        #upcoming_time = datetime.now()+pd.Timedelta(seconds=70)
        # 只要最近的事件未发生，则一直循环打印时间！
        while (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 0:
            if (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 8*60:
                print(f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}")
                time.sleep(60)
            elif (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() < 8*60:
                print('已打开浏览器随时待命！！')
                df = pd.read_excel(r'investing彭博字段表_最终版.xlsx', sheet_name='EP_EVENT(最新)')
                df['Model_Name'].fillna(method='ffill', inplace=True)
                base_url = 'https://cn.investing.com/economic-calendar/'
                list_url = [base_url + i for i in df[df['Model_Name'] == upcoming_event]['Investing_Id']]
                list_table_name = df[df['Model_Name'] == upcoming_event]['Table_Name'].tolist()
                options = webdriver.ChromeOptions()  # 谷歌浏览器
                options.page_load_strategy = 'eager'
                options.add_experimental_option("detach", True)
                options.add_argument("blink-settings=imagesEnabled=false")  # 不加载图片
                options.add_argument("--disable-extensions")  # 禁用插件加载
                options.add_argument("--disable-gpu")  # 禁止gpu
                # options.add_argument("--disable-software-rasterizer")  # 无界面
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                options.add_argument("--start-maximized")  # 窗口全屏显示
    
                drivers = [webdriver.Chrome(options=options) for _ in range(len(list_url))]  # 创建浏览器实例列表
                for driver, url in zip(drivers, list_url):  # 同时打开所有的浏览器
                    driver.get(url)
                while (pd.to_datetime(upcoming_time) - datetime.now()).total_seconds() > 0:
                    print(f"当前时间:{datetime.now()}\t >>>>>>>>>>> \t即将到来的事件:{upcoming_event}\t >>>>>>>>>>> \t事件时间{upcoming_time}\t >>>>>>>>>>> \t剩余时间{(pd.to_datetime(upcoming_time) - datetime.now()).total_seconds()}")
                    time.sleep(2)
            else:
                print(f'你已错过{upcoming_event}事件！')
        # 超过了事件时间，进行抓取
        while True:
            flag_list = []
            # 保存csv到本地文件
            for driver, url ,name in zip(drivers, list_url, list_table_name):
                # 获得正文文本,存在的xpath路径太多太多了！！这部分特别重要！！
                try:
                    data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[12]/table')
                    data_text = data_element.text
                except:
                    try:
                        data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[9]/table')
                        data_text = data_element.text
                    except:
                        try:
                            data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[10]/table')
                            data_text = data_element.text
                        except:
                            try:
                                data_element = driver.find_element(By.XPATH, '/html/body/div[5]/section/div[11]/table')
                                data_text = data_element.text
                            except:
                                try:
                                    data_element = driver.find_element(By.XPATH, '/html/body/div[6]/section/div[12]/table')
                                    data_text = data_element.text
                                except:
                                    try:
                                        data_element = driver.find_element(By.XPATH, '/html/body/div[6]/section/div[9]/table')
                                        data_text = data_element.text
                                    except:
                                        try:
                                            data_element = driver.find_element(By.XPATH,
                                                                               '/html/body/div[6]/section/div[10]/table')
                                            data_text = data_element.text
                                        except:
                                            data_element = driver.find_element(By.XPATH,
                                                                               '/html/body/div[6]/section/div[11]/table')
                                            data_text = data_element.text
                                            
                text = replace_time(data_text)
                text = replace_spaces(text)  # 按照要求进行替换，替换后就可以关闭文件了
    
                #driver.quit()
    			pass #后续代码和总表爬虫一致
    ```

    

- **注意事项**

  1. 不会出现Xpath路径找不到"加载更多"按钮的错误，因为我只需要找最新的数据，而不用以前的老数据
  2. `Investing`这个网站貌似不能发布数据直接投向网页，很可能需要刷新，因此，当获取不到数据的时候需要刷新。但需要注意的是，如果一两分钟后还是显示程序未结束**(程序结束是需要每个表都要拿到最新的数据，可能这次发布的数据并非这个事件下的所有表!**)，建议手动关闭程序，并查看数据库和`Investing`网站

  

- **作者**

  [陈颖航](https://github.com/jason51108)


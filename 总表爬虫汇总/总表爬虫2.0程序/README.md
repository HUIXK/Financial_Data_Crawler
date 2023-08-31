# 总表爬虫程序2.0说明

- **项目背景**

  根据`investing彭博字段表_最终版(新版).xlsx`爬取`Investing`网站对应的数据，并存储到数据库

  

- **程序设计流程：**

  1. 去`Invseting`网站爬取对应总表数据**(剔除掉国债收益率)**，将爬取到的数据保存在`总表数据\初始txt文档`文件夹下
  2. 读取`总表数据\初始txt文档`下的`.txt`文件，进行一定的格式转化**(利用正则表达式进行转化)**，和阿里云数据库中的表进行拼接、去重，拼接好的新表会存放在本地的`处理好后的csv`。
  3. 清空阿里云数据库的表，并将`处理好后的csv`文件夹下的所有`.csv`文件存入数据库

  > 注意：
  >
  > 1、剔除国债收益率的原因在于其`base_url`和财经日历的`base_url`不一样，两个网页的结构也不一样，因此需要单独制定爬虫
  >
  > 2、总表爬虫包含了`Invseting`财经日历下所有的能爬取到的数据，因此也能爬取的到总表的子表`EP_EVENT(最新)`的所有数据。
  >
  > 3、由于设计流程第三步需要清空阿里云数据库的表，因此最好最好在每次程序运行后查看本地文件无误后进行手动备份！




+ **文件说明：**
  + `总表数据`：
    + `初始txt文档`：爬取下来的`.txt`文件
    + `处理好后的csv`：`.txt`表和数据库表经过处理后形成的`.csv`文件
    + `备份.csv`：本地`.csv`的备份，经检查后无误
  + `investing彭博字段表_最终版(新版).xlsx`表：根据这个表去爬取数据
  + `investing彭博字段表_最终版(老版，暂时没用).xlsx`表：已经丢弃，暂时没用
  + `事件总表.py`程序：爬取所有的事件，即总表的子表
  + `指定表名爬虫.py`程序：爬取自定义的表名，需要指定表名
  + `总表.py`程序：总表爬虫，爬取所有的财经日历数据，每日更新一次就好
  + `总表.ipynb`程序：调试代码，如果出现错误，可在上面修改调试



- **安装方法**

  在运行程序之前，请确保安装了以下必要的第三方库

  + Selenium以及对应Google浏览器版本的[Chrome driver](https://googlechromelabs.github.io/chrome-for-testing/)
  + Pandas
  + Numpy

  对于没有相关的依赖库，需要使用`pip install '库名'`或者`conda install '库名'`，**选取解释器时，注意Conda环境下是否存在相关的依赖库，建议使用Conda下的base环境。**

  

+ **各程序说明**

  + `事件总表.py`程序：爬取所有的事件，即总表的子表。
    + 是否完结：~~经8月22日测试，程序已完结~~
    + 程序功能：爬取`Investing`财经日历上的所有总表存为`.txt`，读取`.txt`与`sql`中的表做连接后保存到本地为`.csv`，最后将`.csv`文件存入`.sql`中
    + 注意：因为数据跟新事件比较慢，因此爬取的`.txt`很少就够了！
  + `指定表名爬虫.py`程序：爬取自定义的表名，需要指定表名
    + 是否完结：~~经8月22日测试，程序已完结~~
    + 程序功能：同上，但并非爬取总表，而是指定的表，需要传入表的名称！(如果表名错误，则会报错)
    + 注意：爬取的`.txt`为指定的表名，处理的`.csv`为所有表，上传到`sql`的表为指定的表，后续可以根据需要修改第二部分，即：处理的`.csv`为所有表改为处理的`.csv`为指定的`.csv`

  + `事件总表.py`程序：爬取所有的事件，即总表的子表

    + 是否完结：~~经8月22日测试，程序已完结~~

    + 程序功能：爬取`Investing`财经日历上的所有有关事件的表存为`.txt`，读取`.txt`与`sql`中的表做连接后保存到本地为`.csv`，最后将`.csv`文件存入`.sql`中

    + 注意：因为数据跟新事件比较慢，因此爬取的`.txt`很少就够了！；这个程序和`EP`文件下的程序比较类似，但是少了合并feature特征这一项

      

- **上手指南**

  程序主要定义函数及其注释如下：

  ```python
  # 查询数据库对应表，转化为dataframe
  def read_db_df(table_name):
  	pass
  	
  	
  # 将本地添加到本地数据库
  def append_df_to_db_1(df,table_name):
  	pass
  
  # 将本地添加到阿里云数据库
  def append_df_to_db_2(df,table_name):
  	pass
  
  # 爬虫爬取对应的数据
  def get_text(url,name):
  	pass
  	
  # 正则表达式替换函数1(替换其中的英文时间)
  def replace_time(text):
  	pass
  	
  # 正则表达式替换函数2
  def replace_spaces(text):
  	pass
  	
  #本地的txt文件和数据库文件进行合并，合并后并保存到本地的csv文件夹下
  def txt_to_csv(path):
  	pass
  ```

  > 注意：`txt_to_csv`这个函数下的以下两行代码不要轻易打开！如果因为新增表而打开，请确保文件备份以及暂时未传到数据库！
  >
  > ```python
  > df_2.sort_values(by='datetime', ascending=False, inplace=True)
  > df_2.to_csv(rf'总表数据\处理好后的csv\{name}.csv',index=False)
  > ```

  主程序如下：

  ```python
  # ========主程序
  # 获取需要爬虫爬取的url地址和文件名称
  base_url = 'https://cn.investing.com/economic-calendar/'
  df = pd.read_excel(r'./investing彭博字段表_最终版(新版).xlsx', sheet_name='总表')
  df = df[~df['INVESTING_CN_INDEX'].str.contains('国债收益率')]
  df['INVESTING_INDEX'] = base_url + df['INVESTING_INDEX']
  urllst = df['INVESTING_INDEX'].tolist()
  titlelst = df['TABLE_NAME'].tolist()
  
  # 储存txt文件
  with ThreadPoolExecutor(max_workers=3) as t:  # 创建一个最大容纳数量为3的线程池
      for i, j ,k in zip(urllst, titlelst,range(len(urllst))):
          task = t.submit(get_text, i, j)
          print(f"task{k+1}: {task.done()}")  # 监督进度
  
  # txt转化为csv
  path_1 = rf'总表数据\初始txt文档'
  txt_to_csv(path_1)
  
  #csv存入数据库
  path_2 = rf'总表数据\处理好后的csv'
  path_list = [os.path.join(path_2,i) for i in os.listdir(path_2)]
  for path_ in path_list:
      df = pd.read_csv(path_)
      df.replace('None', None, inplace=True) #将字符串None替换为空
      try:
          df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')  #时间格式处理时间格式转化为指定格式的字符串
      except:
          print(name, '时间格式转换失败')
      name = path_.split('\\')[-1][:-4]
      try:
          append_df_to_db_2(df,name)  #更新到阿里云数据库
          append_df_to_db_1(df,name)  #更新到本地数据库
      except:
          print(f'{name}未加载到数据库！！请注意检查！！')
  ```

  

- **注意事项**

  代码并无大问题，可能错误的地方有两处，错误原因或许如下：

  1. 如果出现Xpath路径错误，未检查到"加载更多"这个按钮

     **解决办法：**因为文件数据已经加载完毕或者网页加载速度较慢，对于程序执行几乎没影响。

  2. 如果出现`.txt`文件转`.csv`文件的错误

     **解决办法：**一般来说都是因为Investing出现问题，从该网站上拿到的`.txt`文件未按照规定的格式，目前来看只有一个表(SMB_MPS)存在这样的问题，这个表在网站上的数据也为空，因此可以完全不用管！

  3. 对于远古年份，或许会出现没有前值(`表名_Prior`值)的情况，这样子通过正则表达式解析后会出错！

     **解决办法：**对于现如今而言，已经不会出现错误了，但是当爬取很久很久以前的数据的时候，对于错误的`.csv`，需要我们手动查看并修改(或者写一个小程序检查一下错误！)。

     

- **作者**

  [陈颖航](https://github.com/jason51108)


package com.haima.sage.bigdata.etl.performance.test

import scala.collection.mutable

object CarNameUtils {
  val brands: Seq[(String, String)] = Seq(
    "AC Schnitzer",
    "宝骐汽车",
    "宝骐",
    "北京清行",
    "北京",
    "清行",
    "北汽昌河",
    "昌河",
    "北汽道达",
    "道达",
    "北汽绅宝",
    "绅宝",
    "北汽幻速",
    "北汽威旺",
    "幻速",
    "威旺",
    "北汽制造",
    "北汽新能源",

    "布加迪",
    "长安跨越",
    "跨越",
    "长安欧尚",
    "欧尚",
    "欧尚汽车",
    "长安轻型车",
    "长安汽车",
    "长安",
    "大乘汽车",
    "大乘",
    "大发",
    "电咖",
    "东风·瑞泰特",
    "东风风行",
    "东风风神",
    "东风风光",
    "东风小康",
    "东风风度",
    "东风标致",
    "标致",
    "东风",
    "光冈",
    "广汽集团",
    "广汽吉奥",
    "广汽传祺",
    "广汽新能源",
    "广汽",
    "国金汽车",
    "海格",
    "恒天",
    "红星汽车",
    "红星",
    "华凯",
    "华骐",
    "华泰新能源",
    "华泰",
    "Icona",
    "江铃集团轻汽",
    "江铃集团新能源",
    "江铃集团",
    "江铃",
    "钧天",
    "KTM",
    "卡尔森",
    "卡升",
    "科尼赛克",
    "LITE",
    "LOCAL MOTORS",
    "Lorinser",
    "劳斯莱斯",
    "理想智造",
    "理想",
    "零跑汽车",
    "零跑",
    "领途汽车",
    "领途",
    "陆地方舟",
    "罗夫哈特",
    "迈巴赫",
    "迈凯伦",
    "摩根",
    "NEVS国能汽车",
    "NEVS",
    "国能",
    "哪吒汽车",
    "哪吒",
    "南京金龙",
    "金龙",
    "欧拉",
    "欧朗",
    "Polestar",
    "帕加尼",
    "前途",
    "乔治·巴顿",
    "庆铃汽车",
    "庆铃",
    "全球鹰",
    "容大智造",
    "容大",
    "如虎",
    "瑞驰新能源",
    "瑞驰",
    "赛麟",
    "陕汽通家",
    "陕汽",
    "通家",
    "世爵",
    "斯达泰克",
    "腾势",
    "威马汽车",
    "威马",
    "威兹曼",
    "蔚来",
    "小鹏汽车",
    "小鹏",
    "新特汽车",
    "新特",
    "鑫源",
    "宇通客车",
    "宇通",
    "御捷",
    "裕路",
    "云度",
    "云雀汽车",
    "云雀",
    "知豆",

    "大众",
    "本田",
    "丰田",
    "奔驰",
    "别克",
    "奥迪",
    "日产",
    "福特",
    "宝马",
    "现代",
    "起亚",
    "雪佛兰",
    "哈弗",
    "马自达",
    "宝骏",
    "斯柯达",

    "吉利汽车",
    "吉利",

    "比亚迪",
    "路虎",
    "五菱汽车",
    "铃木",
    "Jeep",
    "奇瑞",
    "MINI",
    "保时捷",
    "江淮",
    "凯迪拉克",
    "雪铁龙",

    "长城",
    "荣威",
    "雷克萨斯",
    "smart",
    "奔腾",
    "众泰",
    "海马",
    "名爵",

    "三菱",

    "沃尔沃",
    "启辰",
    "捷豹",
    "斯巴鲁",
    "东南",
    "陆风",
    "英菲尼迪",


    "中华",

    "纳智捷",
    "猎豹汽车",
    "猎豹",
    "WEY",

    "雷诺",
    "一汽",

    "菲亚特",
    "玛莎拉蒂",
    "道奇",
    "力帆汽车",
    "力帆",
    "金杯",
    "上汽大通",
    "上汽",
    "大通",
    "驭胜",
    "华泰",
    "林肯",
    "思铭",
    "讴歌",
    "野马汽车",
    // "野马", 和福特mustang重名

    "理念",

    "DS",
    "克莱斯勒",

    "红旗",
    "双龙",
    "江铃",
    "汉腾汽车",
    "汉腾",
    "依维柯",
    "宝沃",
    "领克",
    "凯翼",

    "欧宝",
    "比速汽车",
    "比速",


    "开瑞",
    "宾利",

    "观致",
    "SWM斯威汽车",
    "斯威汽车",
    "斯威",
    "SWM",
    "瑞麒",

    "阿尔法·罗密欧",
    "阿尔法罗密欧",
    "GMC",
    "潍柴英致",
    "潍柴",
    "英致",
    "西雅特",
    "众泰",
    "中兴",
    "莲花汽车",
    "莲花",
    "黄海",
    "福迪",
    "福田乘用车",
    "福田",
    "华颂",
    "阿斯顿·马丁",
    "阿斯顿马丁",
    "永源",
    "巴博斯",
    "君马汽车",
    "君马",
    "福汽启腾",
    "福汽",
    "启腾",
    "九龙",
    "卡威",
    "威麟",
    "兰博基尼",
    "哈飞",
    "新凯",
    "悍马",
    "华普",
    "劳伦士",
    "江南",
    "特斯拉",
    "五十铃",
    "泰卡特",
    "ALPINA",
    "航天",

    "萨博",
    "之诺",
    "路特斯",
    "捷途",
    "成功汽车",
    "成功",
    "双环",
    "金旅",
    "中欧",
    "汇众",
    "法拉利").map(brand => {
    brand.toLowerCase -> brand
  })


  def processBrand(dst: mutable.Map[String, Any]): mutable.Map[String, Any] = {
    val carName = dst.get("car_name") match {
      case Some(x) =>
        x.toString
      case _ =>
        return dst
    }
    val lowerCarName = carName.toString.toLowerCase()

    var index = 0
    while (index < brands.length) {
      val brand = brands(index)

      if (lowerCarName.startsWith(brand._1)) {
        dst += ("brand" -> brand._2)
        val mIndex = carName.indexOf('款')
        if (mIndex != -1) {
          val rIndex = mIndex - 4
          if (rIndex > 0) {
            val mode = carName.substring(brand._1.length, rIndex)
            dst += ("mode" -> mode)
            val year = carName.substring(rIndex, mIndex)
            dst += ("year" -> year)
          }
        }
        return dst
      }
      index += 1
    }
    dst
  }


  def main(args: Array[String]): Unit = {
    val data = Seq(
      "哈弗哈弗H6 Coupe2018款 1.5T 自动 蓝标超豪型前驱",
      "Jeep大切诺基 SRT2012款 6.4 自动 SRT8",
      "MINIMINI JCW2015款 2.0T 自动 JCW",
      "依维柯Power Daily2013款 2.5T 手动 A37客货两用高顶7座 柴油",
      "大众Cross POLO2007款 1.6 自动",
      "依维柯Turbo Daily2017款 2.5T 手动 A35客车中顶5-7座47Z5 柴油",
      "奥迪RS 6旅行车2018款 4.0T 自动 Avant尊享运动限量版",
      "保时捷911 敞篷车2012款3.4 自动 Carrera",
      "铃木天语 尚悦2012款 1.6 手动 实用型升级版",
      "奔腾SENIA R92018款 1.2T 自动 尊贵智悦型",
      "丰田 卡罗拉 2017款 1.2T 自动 GL"
    )
    data.foreach(brand => {
      val map = mutable.Map[String, Any]("car_name" -> brand)
      processBrand(map).foreach(item => {
        println(item._1, item._2)
      })
    })

  }
}

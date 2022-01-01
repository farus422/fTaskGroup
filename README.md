# fTaskGroup套件

### 目錄<span id="目錄"></span>

<a href="#物件方法">物件方法</a><br />
<a href="#物件成員">物件成員</a><br />
<a href="#範例">範例</a><br />

---------------------------------------------------------

### 物件方法<span id="物件方法"></span>&nbsp;&nbsp;&nbsp;&nbsp;<a href="#目錄">(回到目錄)</a>
- 生成一隻魚所需參數：魚的編號、套用的軌跡、時間、座標、角度
- 腳本使用的時間單位固定為：毫秒(ms)
- 除了必備欄位外，其餘欄位皆為可省略

### 物件成員<span id="物件成員"></span>&nbsp;&nbsp;&nbsp;&nbsp;<a href="#目錄">(回到目錄)</a>
- 隊伍NPC的編號
  - A1. 取Objects[].CallScript.NPCSettings[].NPCNumber
  - A2. 取TeamSettingToCall.NPCSettings[].NPCNumber
- 隊伍NPC的激活時距
  - B1. A1、A2 會同時設定 ActiveStep
  - 若B1不存在或B1結果為0，則隨機取 TeamSettingToCall.NPCActiveStepsDefault[]
- NPC的ActiveTime
  - 物件有指定則使用物件設定
  - 若無則取叫用者傳入的 NPCActiveStep * NpcIndex


### 範例<span id="範例"></span>&nbsp;&nbsp;&nbsp;&nbsp;<a href="#目錄">(回到目錄)</a>
```json
{
  "Probability": 250, 必備。腳本被抽中的機率，以權重的方式計算，若設為0則此腳本必定不會被抽中只能被其他腳本呼叫
  "NextScript": 1000, 必備。這個腳本跑完後要等多久才可以抽下個腳本
  
  "TeamSetting": [ 設置本腳本的預設值，當Objects裡面有參數省略時即以此處設定為預設，可使 Objects 內的物件省去許多人工計算的麻煩
    {
      "NPCSettings": [ 結構陣列，隨機取一個
        {
          "NPCNumber": 11, NPC的編號
          "NPCActiveSteps": 500 在沒有指定ActiveTime的物件，會用這個值X物件的Index來當作ActiveTime
        },
        {
          "NPCNumber": 11,
          "NPCActiveSteps": 500
        },
        {
          "NPCNumber": 13,
          "NPCActiveSteps": 0
        }
      ],
      "NPCActiveStepsDefault": [500,300,200], 預設隊伍NPC產生的時距，當NPCSettings[]裡面的NPCActiveSteps為0則會取用此處
      "Speed": 0.5, 隊伍速度，取代魚原本的速度讓整個隊伍速度一致
      "SpeedDouble": 1.5, 隊伍加速，整個隊伍加速
      "XBegin": -50,
      "XEnd": -60,
      "YBegin": 50,
      "YEnd": 550,
      "ZBegin": 0,
      "ZEnd": 0,
      "AngleBegin": -10,
      "AngleEnd": 10
    }
  ],

  "TeamSettingToCall": [ 呼叫其他腳本時才會生效，用來覆蓋被呼叫腳本內的TeamSetting資料，這個結構內有設定的才會覆蓋，沒設定到的就是沿用被呼叫者內的設定
    {
      "Scripts": [ 隨機取一個，當被叫用的腳本內的Scripts為空時可以使用此處的值
        "MotionS.json",
        "Circle.json",
        "Square.json"
      ],
      "NPCSettings": [
        {
          "NPCNumber": 11,
          "NPCActiveSteps": 500
        },
        {
          "NPCNumber": 11,
          "NPCActiveSteps": 500
        },
        {
          "NPCNumber": 13,
          "NPCActiveSteps": 0
        }
      ],
      "NPCActiveStepsDefault": [500],
      "Speed": 0.5,
      "SpeedDouble": 1.5,
      "XBegin": -50,
      "XEnd": -60,
      "YBegin": 50,
      "YEnd": 550,
      "ZBegin": 0,
      "ZEnd": 0,
      "AngleBegin": -10,
      "AngleEnd": 10
    }
  ],

  "Objects": [ 必備。物件列表
    {
      "CallScript": { 指示這個物件單位是呼叫腳本來產生
        "Scripts": [ 叫用腳本，隨機取一個
          "MotionS.json",
          "Circle.json",
          "Square.json"
        ],
        "ScriptsToCall": [ 隨機取一個，當被叫用的腳本內的Scripts為空時可以使用此處的值
          "MotionS.json",
          "Circle.json",
          "Square.json"
        ],
        "NPCSettings": [
          {
            "NPCNumber": 3, 傳入被呼叫腳本的NPC編號
            "NPCActiveSteps": 500 傳入被呼叫腳本的NPCActiveSteps
          },
          {
            "NPCNumber": 5,
            "NPCActiveSteps": 0
          },
          {
            "NPCNumber": 7,
            "NPCActiveSteps": 0
          }
        ],
        "NPCActiveStepsDefault": [500,300] 預設的NPCActiveSteps，當NPCSettings[]裡面的NPCActiveSteps為0則會取用此處
      },
      不管是不是叫用腳本，時間、座標、角度，這三個固定都是放在這個階層
      "ActiveTimeBegin": 0, 物件生成的時間區段(區段開始)，注意：如果沒有設定此欄位，則會變成由ObjectIndex * SetpTime 來計算
      "ActiveTimeEnd": 1000, 物件生成的時間區段(區段結束))，注意：如果沒有設定此欄位，則會變成由ObjectIndex * SetpTime 來計算
      "XBegin": -50,
      "XEnd": -100,
      "YBegin": 0,
      "YEnd": 300,
      "ZBegin": 0,
      "ZEnd": 0,
      "AngleBegin": -20,
      "AngleEnd": 60
    },
    {
      "NPCSettings": [ 指定物件的編號跟軌跡，當CallScript存在時此處無意義可省略，反之則為必備欄位
        {
          "NPCNumber": 11, 生成物件的編號
          "NPCMotion": 1 生成物件的軌跡
        },
        {
          "NPCNumber": 11,
          "NPCMotion": 0
        },
        {
          "NPCNumber": 13,
          "NPCMotion": 0
        }
      ],
      "NPCMotionsDefault": [1, 2, 3], 預設的NPCMotion，當NPCSettings[]裡面的NPCMotion為0則會取用此處
      "ActiveTimeBegin": 2000, 注意：如果沒有設定此欄位，則會變成由ObjectIndex * SetpTime 來計算
      "ActiveTimeEnd": 3000, 注意：如果沒有設定此欄位，則會變成由ObjectIndex * SetpTime 來計算
      "XBegin": -50,
      "XEnd": -100,
      "YBegin": 300,
      "YEnd": 600,
      "ZBegin": 0,
      "ZEnd": 0,
      "AngleBegin": -20,
      "AngleEnd": 60
    }
  ]
}
```

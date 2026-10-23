
import os,json,requests
from datetime import datetime,timezone
from web3 import Web3

RPC="https://rpc.mainnet.chain.robinhood.com"
FACTORY=Web3.to_checksum_address("0x1E3aC31cF96b20619c913384C9bf6010A824fB95")
STATE="state.json"
DATA="scanned_data.json"
TOPIC=Web3.keccak(text="PairCreated(address,address,address,uint256)").hex()

def read(f,d):
    try:
        with open(f,encoding="utf-8") as x:return json.load(x)
    except (OSError,ValueError):return d

def save(f,d):
    with open(f+".tmp","w",encoding="utf-8") as x:json.dump(d,x,ensure_ascii=False)
    os.replace(f+".tmp",f)

def notify(msg):
    k=os.getenv("SERVER_CHAN_KEY","").strip()
    if not k:
        print("缺少SERVER_CHAN_KEY");return
    try:
        r=requests.post("https://sctapi.ftqq.com/"+k+".send",
                        data={"title":"Robinhood新交易池","desp":msg},timeout=15)
        print("微信通知:",r.status_code,r.text[:300])
    except Exception as e:print("通知失败:",e)

def main():
    w=Web3(Web3.HTTPProvider(RPC,request_kwargs={"timeout":30}))
    if not w.is_connected():raise RuntimeError("RPC连接失败")
    if w.eth.chain_id!=4663:raise RuntimeError("Chain ID不正确")
    end=w.eth.block_number
    s=read(STATE,{})
    start=int(s.get("Robinhood",max(0,end-1000)))
    if "Robinhood" not in s:start=max(0,end-999)
    data=read(DATA,[])
    print("连接成功，区块",end,"扫描",start,"到",end)
    while start<=end:
        stop=min(start+499,end)
        logs=w.eth.get_logs({"address":FACTORY,"fromBlock":start,
                             "toBlock":stop,"topics":[TOPIC]})
        for x in logs:
            t=x["topics"]
            if len(t)<3:continue
            b=x["data"]
            if isinstance(b,str):b=bytes.fromhex(b.removeprefix("0x"))
            if len(b)<32:continue
            item={"block":x["blockNumber"],
                  "token0":Web3.to_checksum_address("0x"+t[1].hex()[-40:]),
                  "token1":Web3.to_checksum_address("0x"+t[2].hex()[-40:]),
                  "pair":Web3.to_checksum_address("0x"+b[:32].hex()[-40:]),
                  "tx":x["transactionHash"].hex()}
            data.append(item)
            save(DATA,data)
            print("发现交易池",item)
            notify(json.dumps(item,ensure_ascii=False))
        s["Robinhood"]=stop
        save(STATE,s)
        start=stop+1
    print("扫描完成")

if __name__=="__main__":main()


import os,json,requests
from datetime import datetime,timezone
from web3 import Web3

RPC="https://rpc.mainnet.chain.robinhood.com"
CHAIN_ID=4663
FACTORY=Web3.to_checksum_address("0x1E3aC31cF96b20619c913384C9bf6010A824fB95")
STATE="state.json"
DATA="scanned_data.json"
BATCH=500
FIRST=1000
TOPIC="0x"+Web3.keccak(text="PairCreated(address,address,address,uint256)").hex().removeprefix("0x")

def read(f,default):
    try:
        with open(f,encoding="utf-8") as x:
            return json.load(x)
    except (OSError,ValueError):
        return default

def save(f,obj):
    with open(f+".tmp","w",encoding="utf-8") as x:
        json.dump(obj,x,ensure_ascii=False)
    os.replace(f+".tmp",f)

def notify(title,msg):
    key=os.getenv("SERVER_CHAN_KEY","").strip()
    if not key:
        print("通知失败：未配置 SERVER_CHAN_KEY")
        return False
    try:
        r=requests.post("https://sctapi.ftqq.com/"+key+".send",
                        data={"title":title,"desp":msg},timeout=15)
        print("通知HTTP状态:",r.status_code)
        r.raise_for_status()
        result=r.json()
        print("通知接口返回:",result)
        return result.get("code")==0
    except Exception as e:
        print("通知发送异常:",repr(e))
        return False

def main():
    print("Robinhood新交易池扫描启动")
    w=Web3(Web3.HTTPProvider(RPC,request_kwargs={"timeout":30}))
    if not w.is_connected():
        raise RuntimeError("RPC连接失败")
    if w.eth.chain_id!=CHAIN_ID:
        raise RuntimeError("Chain ID不匹配")
    latest=w.eth.block_number
    state=read(STATE,{})
    saved=state.get("Robinhood")
    start=int(saved)+1 if saved is not None else max(0,latest-FIRST+1)
    print("连接成功，最新区块:",latest,"起始区块:",start)
    records=read(DATA,[])
    known={x.get("tx") for x in records if isinstance(x,dict)}
    found=0

    while start<=latest:
        stop=min(start+BATCH-1,latest)
        print("扫描区块:",start,"-",stop)
        logs=w.eth.get_logs({
            "address":FACTORY,"fromBlock":start,"toBlock":stop,
            "topics":[TOPIC]
        })
        for log in logs:
            tx=log["transactionHash"].hex()
            if tx in known:
                continue
            topics=log["topics"]
            raw=log["data"]
            if isinstance(raw,str):
                raw=bytes.fromhex(raw.removeprefix("0x"))
            if len(topics)<3 or len(raw)<32:
                continue
            item={
                "chain":"Robinhood","dex":"Topaz V2",
                "block":log["blockNumber"],
                "token0":Web3.to_checksum_address("0x"+topics[1].hex()[-40:]),
                "token1":Web3.to_checksum_address("0x"+topics[2].hex()[-40:]),
                "pair":Web3.to_checksum_address("0x"+raw[:32].hex()[-40:]),
                "tx":tx,
                "time":datetime.now(timezone.utc).isoformat()
            }
            records.append(item)
            known.add(tx)
            save(DATA,records)
            found+=1
            msg=json.dumps(item,ensure_ascii=False,indent=2)
            print("发现新池:",msg)
            notify("Robinhood发现Topaz新交易池",msg)

        state["Robinhood"]=stop
        save(STATE,state)
        start=stop+1

    print("本次完成，发现新池:",found)
    if found==0:
        print("本次没有发现新的Topaz V2交易池")

if __name__=="__main__":
    main()

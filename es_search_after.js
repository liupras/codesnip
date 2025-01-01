/*
使用search_after对elasticsearch深度分页
刘立军
2025年1月1日
*/

import axios from 'axios'

// es默认返回文档的窗口10000，如果超过此数量会报错。所以这里的设置应该小于等于10000。
const MaxDocsSize = 10000
//const host = 'localhost:9200'
const host = '10.10.145.111:9200'

const customConfig = {
  headers: {
    "Content-Type": "application/json",
  },
};

// 获取文档数量
export async function countDocumentsBySearch(index, query) {
  let url = 'http://'+ host +'/' + index + '/_count'; 
  console.log(url,query);
    try{
      const response = await axios.post(url,JSON.stringify(query),customConfig)
      return response.data.count;
    }catch (error) {
      console.error("Error counting documents"+index+":", error);
    }
    return 0
}


// 查询分页，适用于返回结果在10000以上
export async function getOnePageFromLarge(index,from,size,query,key,total) {
  if (from < 1) {
    from = 1
  }
  if (size <=0) {
    size = 10
  }
  
  let max_count = from * size
  console.log(from,size,max_count);

  let return_count = size;
  if (max_count > total){
    return_count=max_count-total;
  }

  if (max_count <= MaxDocsSize) {
    let body={
      track_total_hits: true,
      size: max_count,
      sort:[
        {[key]:{order:"asc"}}
      ],
      query:query
    };
    let res = await getOnePageBySearchAfter(index,return_count,body)
    //console.log(res)
    return res[0]
  }else{
    console.log("请求文档大于：",MaxDocsSize);
    let skipCount = Math.floor(max_count/MaxDocsSize);
    let remain = max_count - skipCount * MaxDocsSize;
    if (remain <= 0) {
      remain = MaxDocsSize;
      skipCount = skipCount - 1;
    };
    console.log("count is:",skipCount,remain)

    let body = {
      track_total_hits: true,
      size: max_count,
      sort:[
        {[key]:{order:"asc"}}
      ],
      query:query
    };

    let searchAfter = []

    while (skipCount > 0) {
      body.size = MaxDocsSize
      console.log("还需要跳过:",skipCount,searchAfter);
      if (searchAfter.length > 0) {
        body.search_after = searchAfter;        
      }
      let res = await skipMiddlePageBySearchAfter(index,body)
      if (res.length > 0) {
        searchAfter = res;
      } 
      skipCount = skipCount - 1
    }

    if (searchAfter.length > 0) {
      body.search_after = searchAfter;
      body.size = remain;
    }

    let res = await getOnePageBySearchAfter(index,return_count,body);
    return res[0];
  }
  return []
}

// 查询一页
async function getOnePageBySearchAfter(index,count,body) {

  let url = 'http://'+ host +'/' + index + '/_search'; 
  console.log(body)

  try{ 
    let res = await axios.post(url,JSON.stringify(body),customConfig);    
    let docs = res.data.hits.hits

    let len = docs.length
    if (len >0){
      let sa = docs.slice(-1)[0]["sort"];

      if (len <= count) {
        return [docs,sa];
      }else{
        return [docs.slice(-count),sa];
      }
    }
  } catch (error) {
    console.error("Error searching documents"+index+":", error);
  }
  return [[],[]] ;
}

// 跳过中间页
async function skipMiddlePageBySearchAfter(index,body) {
  let url = 'http://'+ host +'/' + index + '/_search'; 
  console.log(body) 

  try{ 
    let res = await axios.post(url,JSON.stringify(body),customConfig);    
    let docs = res.data.hits.hits
    let len = docs.length
    if (len > 0){
      let sa = docs.slice(-1)[0]["sort"];
      return sa;      
    }
  } catch (error) {
    console.error("Error searching documents"+index+":", error);
  }
  return [] ;
}       
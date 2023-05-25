import { useEffect, useState } from "react"
import './fundingRankStyle.css'
const EXCHANGE_LIST = ['okx', 'bybit', 'bitmex']


function getDataFromApi(exchange) {
  return fetch(`http://139.59.154.21:8080/fundingRank?cex=${exchange}`)
    .then(response => response.json())
}


function FundingRankTable() {
  const [fundingRank, setFundingRank] = useState([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    async function fetchData() {
      try {
        const promises = EXCHANGE_LIST.map(exchange => getDataFromApi(exchange))
        const results = await Promise.all(promises)

        const parsedData = {}
        results.forEach((json, index) => {
          parsedData[EXCHANGE_LIST[index]] = json
        })

        setFundingRank(parsedData)
      } catch (error) {
        console.error("Error fetching data:", error)
      } finally {
        setLoading(false)
      }
    }
    fetchData()
  }, [])

  return (
    <div className="FundingRankTable_div">
      {loading ? (
        <div>Loading...</div>
      ) : (
        <>
          {Object.entries(fundingRank).map(([exchange, data]) => (
            <div key={exchange}>
              <h1>{exchange}</h1>
              <table className="tableFunding" border={1}>
                <tr>
                  <th>Pair</th>
                  <th>Funding</th>
                </tr>
                {data.map(user => (
                  <tr key={user.id}>
                    <td>{user.pair}</td>
                    <td>{user.funding}</td>
                  </tr>
                ))}
              </table>
            </div>
          ))}
        </>
      )}
    </div>
  )
}
  

export default FundingRankTable; 
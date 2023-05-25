
import React, { Component } from 'react';
import logo from './logo-cryptoTrendz.png';
import './App.css';
import FundingRankTableOkx from './components/FundingRankTable/FundingRankTable'

class App extends Component {
  render() {
    return (
      <div className="App">
        <div className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <div className='text-logo'>
            <h3>CryptoTrendz is a data-driven platform designed to optimize trading strategies</h3>
            <p>you can leverage these advantages to enhance your trading skills, optimize your strategies, and make more informed decisions in the dynamic and volatile world of cryptocurrency trading.</p>
          </div>
        </div>
        <div className="div-fundingRank">
          <h2 className='fundingTableTitle'>Funding Return current Week</h2>
          <FundingRankTableOkx></FundingRankTableOkx>
          <div className='info-section'>
            <p className='warning'>
              WARNING: this is only a test version of the app the data could be incorrect, to be sure reload the page twice.
            </p>
            <h3>
              WHAT IS THAT?
            </h3>
            <p>
              Those tables represents the sum of 2 months of funding rate.
              At the moment is visualized the top 4 pair with the highest positive funding rate.
            </p>
          </div>
        </div>
      </div>
    );
  }
}

export default App;
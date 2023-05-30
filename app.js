$(document).ready(function() {

    // Function to fetch the data for the histogram
    function fetchHistogramData() {
      $.ajax({
          url: '/histogram',
          method: 'GET',
          success: function(data) {
              // Check if the chart already exists and destroy it
              if (window.histogram) {
                  window.histogram.destroy();
              }
  
              // Create the histogram using Chart.js
              var ctx = document.getElementById('avg-duration-histogram').getContext('2d');
              window.histogram = new Chart(ctx, {
                  type: 'bar',
                  data: {
                      labels: data.labels,
                      datasets: [{
                          label: 'Duration (sec)',
                          data: data.data,
                          backgroundColor: 'rgba(255, 99, 132, 0.2)',
                          borderColor: 'rgba(255, 99, 132, 1)',
                          borderWidth: 1
                      }]
                  },
                  options: {
                      scales: {
                        x: {
                          ticks: {
                            stepSize: 500
                          },
                          max: 8000,
                          beginAtZero: true
                        },
                        y: {
                          beginAtZero: true
                        }
                      }
                  }
              });
          },
          error: function(error_data) {
              console.log(error_data);
          }
      });
    }
  
    // Function to fetch the data for the mqtt_msg table
    function fetchMqttMsgData() {
      $.ajax({
          url: '/mqtt_msg',
          method: 'GET',
          success: function(data) {
              // Update the table with the fetched data
              var mqtt_table = $('#mqtt-table tbody');
              mqtt_table.empty();
              for (var i = 0; i < data.length; i++) {
                  var row = '<tr><td>' + data[i][0] + '</td></tr>';
                  mqtt_table.append(row);
              }
          },
          error: function(error_data) {
              console.log(error_data);
          }
      });
    }
  



    function fetchPopularStationData() {
        $.ajax({
            url: '/popular_station',
            method: 'GET',
            success: function(data) {
                // Update the table with the fetched data
                var table = $('#popular-station-table tbody');
                table.empty();
                for (var i = 0; i < data.length; i++) {
                    var row = '<tr><td>' + data[i]['start_station_name'] + '</td><td>' + data[i]['end_station_name'] + '</td><td>' + data[i]['count'] + '</td></tr>';
                    table.append(row);
                }
            },
            error: function(error_data) {
                console.log(error_data);
            }
        });
    }
    
 


    function fetchPopularBikesData() {
        $.ajax({
            url: '/popular_bikes',
            method: 'GET',
            success: function(data) {
                // Update the table with the fetched data
                var popular_bikes_table = $('#popular-bikes-table tbody');
                popular_bikes_table.empty();
                for (var i = 0; i < data.length; i++) {
                    var row = '<tr><td>' + data[i].bike_id + '</td><td>' + data[i].count + '</td></tr>';
                    popular_bikes_table.append(row);
                }
            },
            error: function(error_data) {
                console.log(error_data);
            }
        });
    }
    







    
    // Function to fetch both the histogram and mqtt_msg data
    function fetchData() {
      fetchHistogramData();
      fetchMqttMsgData();
      fetchPopularStationData();
      fetchPopularBikesData();
    }
  
    // Fetch the data initially when the page is loaded
    //fetchData();
  
    // Set up an interval to fetch the data periodically
    setInterval(fetchHistogramData, 2000);
    setInterval(fetchMqttMsgData, 3000);
    setInterval(fetchPopularStationData, 5000);
    setInterval(fetchPopularBikesData, 7000);
  
  });
  
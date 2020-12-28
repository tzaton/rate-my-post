// Load forum names
$(document).ready(function () {
    $.get('forums.txt', function (data) {
        var lines = data.split("\n");
        lines.sort();
        $.each(lines, function (index, value) {
            $('#forumName').append('<option value="' + value + '">' + value + '</option>');
        });

    });
});


// API parameters
var region = "us-east-1";
var stageName = "prod";
var apiId = "8ppkvrnh0b";
var baseUrl = `https://${apiId}.execute-api.${region}.amazonaws.com/${stageName}/`;


// Run lambda function on button click
$(document).ready(function () {
    $("#submitPost").click(function (event) {
        event.preventDefault();
        var form = $("#userForm")[0];
        if (form.checkValidity() === false) {
            event.stopPropagation();
        }
        else {
            var inputData = {
                "forumName": $("#forumName").val(),
                "userName": $("#userName").val(),
                "postTitle": $("#postTitle").val(),
                "postBody": $("#postBody").val(),
                "postTags": $("#postTags").val()
            };

            $.ajax({
                url: baseUrl,
                type: 'POST',
                dataType: "json",
                crossDomain: "true",
                data: JSON.stringify(inputData),
                contentType: 'application/json; charset=utf-8',
                success: function (response) {
                    var responsePercentage = parseFloat(response).toPrecision(4) * 100 + "%";
                    var result = `<div class="card bg-light"><div class="card-body">Estimated probability of your question being answered is <b>~${responsePercentage}</b></div></div>`;
                    $("#postResponse").html(result);
                },
                error: function () {
                    alert("ERROR");
                }
            });
        }
        form.classList.add('was-validated');
    });
});

var currentPage;
var keywords;
var pageN;
var pageMax;
var pageTotal;

$('#toggleSearch').click(function() {
	currentPage = 0;
    clearPageList();
    keywords = $('#main-search-box').val();
    console.log("Toggle Search, text = "+keywords);
    if (!keywords) return;

    // Animations
    $('#first h1').fadeOut('slow');
    $('.search-animation').css("transition", "0.6s");
    $('#first .header-content').addClass("with-result");
    $('body nav').addClass("with-result");
    $('#dynamic-text').remove();
    $('.search-result').show();
    floatingEnable = false;
    $('.search-animation').css("transition", "0s");
    $('.lds-ripple').show();

    getSearch(keywords);



});

function getSearch() {
    var searchURL = encodeURI("/search/"+keywords+"/"+currentPage);
    console.log(searchURL);
    $('.lds-ripple').show();
    // Search API
//    $.ajax({
//        url: searchURL,
//        context: document.body
//      })
    $.get(searchURL, function(res, status) {
        console.log(res);
        pageTotal = res.total;
        updatePageList(res.pages, currentPage, pageTotal);
        $('.lds-ripple').hide();
    })
    .fail(function(err) {
        console.log(err);
        cannotFound();
        $('.lds-ripple').hide();
    });
}


function updatePageList(data, page, pageTotal) {
    console.log(data);
    var i;
    $(".search-result ul").html("");
    for (i in data) {
        var title = data[i].title;
        var url = data[i].url;
        var display_url = url;
        if (display_url.length > 80)
            display_url = display_url.substring(0, 80)+"...";
        if (title=="") title = display_url;
        var sample = data[i].sample;
        if (sample.length > 300)
            sample = sample.substring(0, 300);

        var html = "<li><h3 class=\"title-link\"><a href=\""+url+"\">"+title
        +"</a></h3><p href=\""+url+"\" class=\"url-link\">"+display_url+"</p>"
        +"<p class=\"sample-text\">"+sample+"</p></li>";
        $(".search-result ul").append(html);
    }

    pageN = ((currentPage)%7+1);
    pageMax = (pageTotal-1)%7+1;

    var html = (pageN==1?"":"<a id=\"prevPage\">Prev</a>")
              +"| Page <span id=\"pageN\">"+pageN+"</span> |"
                +(pageN==pageMax?"":"<a id=\"nextPage\">Next</a>");
    $('.pager').html(html);
    $("#prevPage").click(function() {
        currentPage -= 7;
        if (currentPage < 0) currentPage = 0;
        getSearch();
    });

    $("#nextPage").click(function() {
        currentPage += 7;
        if (currentPage > pageTotal-1) currentPage = pageTotal-1;
        getSearch();
    });

}

function clearPageList() {
    $(".search-result ul").html("");
}


function cannotFound() {
    $(".search-result ul").html("<p>No Result Found.</p>");
}

$(".lucky-btn").click(function() {
    alert("Well ..... Good for you...");
});

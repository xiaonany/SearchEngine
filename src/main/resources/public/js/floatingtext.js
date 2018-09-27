// Configuring dynamic texts
texts=[
"machine learning",
"apple",
"Tensorflow",
"software engineering",
"upenn courses",
"Eat and drink",
"iphone",
"pinterest",
"Photo",
"google search"
];
posX=[0.18,0.30,0.33,0.10,0.57,0.31,0.31,0.15,0.12,0.06];
posY=[0.25,0.30,0.52,0.71,0.83,0.64,0.18,0.19,0.97,0.40];
dist=[0.90,0.30,0.15,0.73,0.88,0.65,0.08,0.43,0.24,0.80];
// dist 1:close -> 0:far
biasX=[0,0,0,0,0,0,0,0,0,0];
biasY=[0,0,0,0,0,0,0,0,0,0];
dbiasX=[0,0,0,0,0,0,0,0,0,0];
dbiasY=[0,0,0,0,0,0,0,0,0,0];

floatingEnable = true;

var stage = $('#dynamic-text');
// Show the stage
$(stage).css("display", "block");

var dt_height = stage.height();
var dt_width = stage.width();
console.log(dt_height+" "+dt_width);

// Generate texts
for (var i in texts) {

    $(stage).append("<a class='floating-dt' id='dt"+i+"'>"+texts[i]+"</a>");
    if (i < texts.length/2) {
        $("#dt"+i).css({"left":posX[i]*dt_width, "right":"auto"});
    }
    else {
        $("#dt"+i).css({"right":posX[i]*dt_width, "left":"auto"});
    }
    $("#dt"+i).css({
        "top": posY[i]*dt_height,
        "font-size": (dist[i]*30+5)
    })
    .css("-webkit-filter","blur("+3*(1-dist[i])+"px);")
    .css("-moz-filter","blur("+3*(1-dist[i])+"px);")
    .css("-o-filter","blur("+3*(1-dist[i])+"px);")
    .css("-ms-filter","blur("+3*(1-dist[i])+"px);")
    .css("filter","blur("+3*(1-dist[i])+"px");

}


// Track the mouse movement
var x = dt_width/2,y = dt_height/2;
$('body').mousemove(function(event) {
    if (!floatingEnable) return;
        x = event.pageX;
        y = event.pageY;

});

window.requestAnimationFrame(function animation() {
    if (!floatingEnable) return;
    movementX = -(x / dt_width * 60 - 30);
    movementY = -(y / dt_height * 60 - 30);
    for (var i in texts) {
        dbiasX[i] += (Math.random()-0.5)/30;
        dbiasY[i] += (Math.random()-0.5)/30;
        biasX[i] += dbiasX[i];
        biasY[i] += dbiasY[i];
        if (biasX[i] > 40 || biasX[i] < -40) dbiasX[i] = -dbiasX[i];
        if (biasY[i] > 40 || biasY[i] < -40) dbiasY[i] = -dbiasY[i];

        $("#dt"+i).css("transform", "translate3d("
            +(movementX+biasX[i])*Math.pow(dist[i],2)
            +"px,"+(movementY+biasY[i])*Math.pow(dist[i],2)+"px,0px)");
    }
    
    window.requestAnimationFrame(animation);

});
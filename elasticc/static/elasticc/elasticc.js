hideOrShow = function( buttonid, divid, displaytype="block" ) {
    button = document.getElementById( buttonid );
    div = document.getElementById( divid );

    if ( div.style.display == "none" ) {
        button.innerHTML = "Hide";
        div.style.display = displaytype;
    }
    else {
        button.innerHTML = "Show";
        div.style.display = "none";
    }
}

hideOrShow = function( buttonid, divid, displaytype="block" ) {
    let hotbutton = document.getElementById( buttonid );
    let hotdiv = document.getElementById( divid );
    
    if ( hotdiv.style.display == "none" ) {
        hotbutton.innerHTML = "Hide";
        hotdiv.style.display = displaytype;
    }
    else {
        hotbutton.innerHTML = "Show";
        hotdiv.style.display = "none";
    }

    // Good idea, but doesn't work as is because of nesting
    // for ( let button of document.getElementsByTagName( "button" ) ) {
    //     if ( ( button.id == hotbutton.id ) ||
    //          ( button.id.substring( 0, 8 ) != 'hideshow' ) ) {
    //         continue;
    //     }
    //     let div = document.getElementById( button.id.substring( 0, button.id.length-6 ).concat( "div" ) );
    //     button.innerHTML = "Show";
    //     div.style.display = "none";
    // }
}

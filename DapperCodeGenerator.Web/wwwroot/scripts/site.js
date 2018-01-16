function selectElementContents(el) {
    if (document.body.createTextRange) {
        const textRange = document.body.createTextRange();
        textRange.moveToElementText(el);
        textRange.select();
        textRange.execCommand('Copy');
    }
    else if (window.getSelection && document.createRange) {
        const range = document.createRange();
        range.selectNodeContents(el);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
        try {
            const successful = document.execCommand('copy');
            const msg = successful ? 'successful' : 'unsuccessful';
            console.log(`Copy ${msg}`);
        } catch (err) {
            console.log('Copy unsuccessful');
        }
    }
}

function make_copy_button(el, iconOnly) {
    const copyBtn = document.createElement('button');
    copyBtn.classList.add('btn', 'btn-sm', 'btn-info');
    el.parentNode.insertBefore(copyBtn, el.nextSibling);
    copyBtn.onclick = function () { selectElementContents(el); };

    // IE 4+, Chrome 42+, Firefox 41+, Opera 29+
    const icon = '<i class="material-icons md-18 align-middle">content_copy</i>';

    if (iconOnly) {
        copyBtn.classList.add('ml-2');
        copyBtn.innerHTML = icon;
    } else {
        copyBtn.classList.add('mt-2');
        copyBtn.innerHTML = `Copy Code <span>${icon}</span>`;
    }

    // Safari, older Chrome, Firefox and Opera
    if (!document.queryCommandSupported('copy') && parseInt(navigator.userAgent.match(/Chrom(e|ium)\/([0-9]+)\./)[2]) < 42) {
        copyBtn.disabled = true;
    }
}

document.querySelectorAll('h5 code').forEach((el) => {
    make_copy_button(el, true);
});

document.querySelectorAll('pre code').forEach((el) => {
    make_copy_button(el, false);
});
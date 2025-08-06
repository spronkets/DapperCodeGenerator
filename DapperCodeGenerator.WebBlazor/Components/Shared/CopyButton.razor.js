export function copyElementTextById(elementId) {
    const el = document.getElementById(elementId);
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
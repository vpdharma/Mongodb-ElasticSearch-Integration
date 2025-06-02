const validateSearch = (req, res, next) => {
    const { q, size, from, fuzzy, fuzziness } = req.query;

    if (!q || q.trim().length === 0) {
        return res.status(400).json({
            error: 'Query parameter "q" is required and cannot be empty'
        });
    }

    if (size && (isNaN(size) || parseInt(size) < 1 || parseInt(size) > 100)) {
        return res.status(400).json({
            error: 'Size must be a number between 1 and 100'
        });
    }

    if (from && (isNaN(from) || parseInt(from) < 0)) {
        return res.status(400).json({
            error: 'From must be a non-negative number'
        });
    }

    if (fuzzy && !['true', 'false'].includes(fuzzy.toLowerCase())) {
        return res.status(400).json({
            error: 'Fuzzy must be true or false'
        });
    }

    if (fuzziness && !['AUTO', '0', '1', '2'].includes(fuzziness)) {
        return res.status(400).json({
            error: 'Fuzziness must be AUTO, 0, 1, or 2'
        });
    }

    next();
};

const validateAutocomplete = (req, res, next) => {
    const { q, size } = req.query;

    if (!q || q.trim().length === 0) {
        return res.status(400).json({
            error: 'Query parameter "q" is required and cannot be empty'
        });
    }

    if (size && (isNaN(size) || parseInt(size) < 1 || parseInt(size) > 20)) {
        return res.status(400).json({
            error: 'Size must be a number between 1 and 20'
        });
    }

    next();
};

module.exports = {
    validateSearch,
    validateAutocomplete
};

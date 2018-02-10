<?php
session_start();
if (!empty($_GET['token'])) {
    $token = $_GET['token'];
    $_SESSION['token'] = $_GET['token'];
} elseif (!empty($_SESSION['token'])) {
    $token = $_SESSION['token'];
} else {
    exit('You must provide a token.');
}
$config = [
    'dataset' => 'privamov',
    'accessToken' => $token,
    'apiEndpoint' => 'api.php',
];
?>
<!doctype html>
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Priva'Mov Viz</title>
    <link rel="stylesheet" href="css/bootstrap.min.css">
    <link rel="stylesheet" href="css/leaflet.css">
    <link rel="stylesheet" href="css/react-datetime.css">
    <link rel="stylesheet" href="css/app.css">
    <script>
        window.Viz = <?php echo json_encode($config) ?>;
    </script>
</head>
<body>
<body style="padding-top: 70px;">
<div id="body"></div>
<script src="bundle.js"></script>
</body>
</html>
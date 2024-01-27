import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;

public class ImageAnonymizer_Distributable {

    Process pythonProcess;
    BufferedInputStream in;
    BufferedOutputStream out;

    BufferedReader err;

    private boolean hasGui;
    private JFrame mainFrame;

    public ImageAnonymizer_Distributable() {
        initialize();
    }

    private void initialize() {
        try {
            // D:\BA\Images\tracking_train1_v1.1\argoverse-tracking\train1\10b8dee6-778f-33e4-a946-d842d2d9c3d7\ring_front_center\ring_front_center_315968234471778960.jpg

            String pythonPath = ""; // TODO: Add path to your python environment here
            String mainPath = ""; // TODO: Add path to python main file here
            pythonProcess = Runtime.getRuntime().exec("%s %s".formatted(pythonPath, mainPath));

            in = new BufferedInputStream(pythonProcess.getInputStream());
            out = new BufferedOutputStream(pythonProcess.getOutputStream());
            err = new BufferedReader(new InputStreamReader(pythonProcess.getErrorStream()));


            // byte[] b = intToBytes(876543210);
            // System.out.println(Arrays.toString(b));

//			System.out.println("Error output:\n");
//            while ((s = err.readLine()) != null) {
//                System.out.println(s);
//            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte[] generate(byte[] bytes) {
        try {
            out.write(intToBytes(bytes.length));
            out.write(bytes);
            out.flush();

            byte[] header = in.readNBytes(4); // might be < 4
            int fileLength = bytesToInt(header);
            byte[] resultContent = in.readNBytes(fileLength);

//			System.out.println("Error output:\n");
//			String s;
//	        while ((s = err.readLine()) != null) {
//	            System.out.println(s);
//	        }

            if (hasGui) {
                buildFrame(imageFromBytes(resultContent));
            }

            return resultContent;

//	        try (FileOutputStream fos = new FileOutputStream("C:/Users/domeh/Desktop/test.jpg")) {
//	     	   fos.write(resultContent);
//	     	   System.out.println("File was written");
//	        }

        } catch (IOException ioe) {
            ioe.printStackTrace();
            return null;
        }

    }

    public void showGUI() {
        this.hasGui = true;

        this.mainFrame = new JFrame("Image");
        mainFrame.setVisible(true);
        mainFrame.setLocationRelativeTo(null);

        buildFrame(null);
    }

    private void buildFrame(BufferedImage image) {
        Box mainContent = Box.createVerticalBox();
        if (image != null)
            mainContent.add(new JLabel(new ImageIcon(image)));

        mainFrame.setContentPane(mainContent);
        mainFrame.pack();
    }

    private static byte[] intToBytes(int value) {
        return new byte[]{(byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value};
    }

    private static int bytesToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) | ((bytes[2] & 0xFF) << 8) | ((bytes[3] & 0xFF) << 0);
    }

    private static BufferedImage imageFromBytes(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try {
            return ImageIO.read(bais);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        // DEMO
        File testfile = new File(""); // TODO: Path to your Fig here
        byte[] testFileContent = Files.readAllBytes(testfile.toPath());

        JFrame frame = new JFrame("Image");
        frame.setVisible(true);
        Box initContent = Box.createVerticalBox();
        initContent.add(new JLabel("Initialized successfully. Waiting for images."));
        frame.setContentPane(initContent);
        frame.pack();
        frame.setLocationRelativeTo(null);

        //Thread.sleep(10000);

        ImageAnonymizer_Distributable ia = new ImageAnonymizer_Distributable();
        ia.generate(testFileContent);

        long start = System.currentTimeMillis();
        long b = 0;

        while (b < 206) {


            String formatted = String.format("%03d", b);
            //String name = "scene" + formatted + ".png";
            String name = b + ".png";

            File file = new File( name); // TODO: Adjust the name of the figs here
            b += 1;

            BufferedImage image;
            byte[] fileContent = Files.readAllBytes(file.toPath());
            image = imageFromBytes(ia.generate(fileContent));

            Box content = Box.createVerticalBox();
            content.add(new JLabel(new ImageIcon(image)));

            frame.setContentPane(content);
            frame.pack();
            Thread.sleep(1000);
        }


//		System.out.println(ia.generate(fileContent));
//		System.out.println(ia.generate(fileContent));


    }

}
